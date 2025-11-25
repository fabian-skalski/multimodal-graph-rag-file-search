"""Rate limiting business logic using token bucket algorithm."""
import logging
import os
import time
from typing import Optional, Dict

import redis

logger = logging.getLogger(__name__)


class RateLimiterService:
    """Service for token bucket rate limiting."""
    
    def __init__(self):
        """Initialize rate limiter service."""
        self.redis_client: Optional[redis.Redis] = None
    
    def initialize(self):
        """Initialize Redis client and default bucket if configured."""
        if self.redis_client is None:
            redis_host = os.getenv("REDIS_HOST")
            redis_port = int(os.getenv("REDIS_PORT"))
            try:
                self.redis_client = redis.Redis(
                    host=redis_host,
                    port=redis_port,
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_timeout=5
                )
                # Test the connection
                self.redis_client.ping()
                logger.info(f"Connected to Redis at {redis_host}:{redis_port}")
                
                # Initialize default bucket if environment variables are set (idempotent)
                bucket_id = os.getenv("RATE_LIMIT_BUCKET_ID")
                capacity_str = os.getenv("RATE_LIMIT_CAPACITY")
                refill_rate_str = os.getenv("RATE_LIMIT_REFILL_RATE")
                
                if bucket_id and capacity_str and refill_rate_str:
                    try:
                        capacity = int(capacity_str)
                        refill_rate = float(refill_rate_str)
                        self._initialize_bucket_idempotent(bucket_id, capacity, refill_rate)
                    except Exception as e:
                        logger.warning(f"Failed to initialize default bucket: {e}")
                        
            except Exception as e:
                logger.exception("Failed to connect to Redis")
                self.redis_client = None
                raise
    
    def health_check(self) -> bool:
        """Check if Redis connection is healthy.
        
        Returns:
            True if Redis is accessible, False otherwise
        """
        try:
            if self.redis_client is None:
                return False
            self.redis_client.ping()
            return True
        except Exception as e:
            logger.exception("Redis health check failed")
            return False
    
    def _initialize_bucket_idempotent(
        self,
        bucket_id: str,
        capacity: int,
        refill_rate: float
    ) -> Dict:
        """Initialize a token bucket only if it doesn't exist (idempotent).
        
        This method is safe to call multiple times or from multiple replicas.
        It only creates the bucket if it doesn't already exist.
        
        Args:
            bucket_id: Unique identifier for the bucket
            capacity: Maximum number of tokens
            refill_rate: Tokens added per second
            
        Returns:
            Bucket configuration
        """
        if self.redis_client is None:
            raise RuntimeError("Redis client not initialized")
        
        bucket_key = f"bucket:{bucket_id}"
        
        # Check if bucket already exists
        if self.redis_client.exists(bucket_key):
            logger.info(f"Bucket '{bucket_id}' already exists, skipping initialization")
            bucket_data = self.redis_client.hgetall(bucket_key)
            return {
                "status": "already_exists",
                "bucket_id": bucket_id,
                "capacity": float(bucket_data.get("capacity", capacity)),
                "refill_rate": float(bucket_data.get("refill_rate", refill_rate))
            }
        
        # Create bucket atomically using Redis transaction
        with self.redis_client.pipeline() as pipe:
            try:
                # Watch the key to detect if another process creates it
                pipe.watch(bucket_key)
                
                # Double-check it doesn't exist
                if pipe.exists(bucket_key):
                    logger.info(f"Bucket '{bucket_id}' was created by another process")
                    pipe.unwatch()
                    bucket_data = self.redis_client.hgetall(bucket_key)
                    return {
                        "status": "already_exists",
                        "bucket_id": bucket_id,
                        "capacity": float(bucket_data.get("capacity", capacity)),
                        "refill_rate": float(bucket_data.get("refill_rate", refill_rate))
                    }
                
                # Create the bucket
                pipe.multi()
                pipe.hset(bucket_key, mapping={
                    "capacity": capacity,
                    "refill_rate": refill_rate,
                    "tokens": capacity,
                    "last_refill": time.time()
                })
                pipe.execute()
                
                logger.info(
                    f"Initialized bucket '{bucket_id}' with capacity {capacity}, "
                    f"refill_rate {refill_rate}"
                )
                
                return {
                    "status": "created",
                    "bucket_id": bucket_id,
                    "capacity": capacity,
                    "refill_rate": refill_rate
                }
                
            except redis.WatchError:
                # Another process created the bucket while we were working
                logger.info(f"Bucket '{bucket_id}' was created by another process during initialization")
                bucket_data = self.redis_client.hgetall(bucket_key)
                return {
                    "status": "already_exists",
                    "bucket_id": bucket_id,
                    "capacity": float(bucket_data.get("capacity", capacity)),
                    "refill_rate": float(bucket_data.get("refill_rate", refill_rate))
                }

    
    def initialize_bucket(
        self,
        bucket_id: str,
        capacity: int,
        refill_rate: float
    ) -> Dict:
        """Initialize or update a token bucket.
        
        Args:
            bucket_id: Unique identifier for the bucket
            capacity: Maximum number of tokens
            refill_rate: Tokens added per second
            
        Returns:
            Bucket configuration
        """
        if self.redis_client is None:
            raise RuntimeError("Redis client not initialized")
        
        bucket_key = f"bucket:{bucket_id}"
        
        self.redis_client.hset(bucket_key, mapping={
            "capacity": capacity,
            "refill_rate": refill_rate,
            "tokens": capacity,
            "last_refill": time.time()
        })
        
        logger.info(
            f"Initialized bucket '{bucket_id}' with capacity {capacity}"
        )
        
        return {
            "status": "success",
            "bucket_id": bucket_id,
            "capacity": capacity,
            "refill_rate": refill_rate
        }
    
    def _refill_bucket(self, bucket_key: str) -> Dict:
        """Refill bucket tokens based on elapsed time.
        
        Args:
            bucket_key: Redis key for the bucket
            
        Returns:
            Updated bucket data
        """
        bucket_data = self.redis_client.hgetall(bucket_key)
        
        if not bucket_data:
            raise ValueError("Bucket not found")
        
        capacity = float(bucket_data["capacity"])
        refill_rate = float(bucket_data["refill_rate"])
        tokens = float(bucket_data["tokens"])
        last_refill = float(bucket_data["last_refill"])
        
        now = time.time()
        elapsed = now - last_refill
        tokens_to_add = elapsed * refill_rate
        
        new_tokens = min(capacity, tokens + tokens_to_add)
        
        # Update bucket
        self.redis_client.hset(bucket_key, mapping={
            "tokens": new_tokens,
            "last_refill": now
        })
        
        return {
            "capacity": capacity,
            "refill_rate": refill_rate,
            "tokens": new_tokens,
            "last_refill": now
        }
    
    def consume_tokens(
        self,
        bucket_id: str,
        tokens: int,
        timeout: Optional[float] = None
    ) -> Dict:
        """Consume tokens from bucket with optional timeout.
        
        Args:
            bucket_id: Identifier for the token bucket
            tokens: Number of tokens to consume
            timeout: Maximum time to wait for tokens (seconds)
            
        Returns:
            Consumption result
            
        Raises:
            ValueError: If bucket not found
            TimeoutError: If timeout exceeded
        """
        if self.redis_client is None:
            raise RuntimeError("Redis client not initialized")
        
        bucket_key = f"bucket:{bucket_id}"
        start_time = time.time()
        
        while True:
            # Use Redis transaction to ensure atomic operation
            with self.redis_client.pipeline() as pipe:
                try:
                    pipe.watch(bucket_key)
                    
                    # Refill bucket
                    bucket_data = self._refill_bucket(bucket_key)
                    available_tokens = bucket_data["tokens"]
                    
                    if available_tokens >= tokens:
                        # Consume tokens
                        pipe.multi()
                        pipe.hset(
                            bucket_key,
                            "tokens",
                            available_tokens - tokens
                        )
                        pipe.execute()
                        
                        logger.debug(
                            f"Consumed {tokens} tokens from '{bucket_id}', "
                            f"{available_tokens - tokens:.0f} remaining"
                        )
                        
                        return {
                            "status": "success",
                            "consumed": tokens,
                            "remaining": available_tokens - tokens
                        }
                    
                except redis.WatchError:
                    # Retry if another client modified the bucket
                    continue
            
            # Check timeout
            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    logger.warning(
                        f"Timeout waiting for {tokens} tokens in '{bucket_id}'"
                    )
                    raise TimeoutError(
                        f"Rate limit timeout after {elapsed:.1f}s"
                    )
            
            # Wait before retrying
            time.sleep(0.1)
    
    def get_available_tokens(self, bucket_id: str) -> Dict:
        """Get available tokens in bucket.
        
        Args:
            bucket_id: Identifier for the token bucket
            
        Returns:
            Bucket status with available tokens
        """
        bucket_key = f"bucket:{bucket_id}"
        bucket_data = self._refill_bucket(bucket_key)
        
        return {
            "bucket_id": bucket_id,
            "available_tokens": bucket_data["tokens"],
            "capacity": bucket_data["capacity"],
            "refill_rate": bucket_data["refill_rate"]
        }
    
    def reset_bucket(self, bucket_id: str) -> Dict:
        """Reset a token bucket to full capacity.
        
        Args:
            bucket_id: Identifier for the token bucket
            
        Returns:
            Reset confirmation
        """
        bucket_key = f"bucket:{bucket_id}"
        
        bucket_data = self.redis_client.hgetall(bucket_key)
        if not bucket_data:
            raise ValueError("Bucket not found")
        
        capacity = float(bucket_data["capacity"])
        
        self.redis_client.hset(bucket_key, mapping={
            "tokens": capacity,
            "last_refill": time.time()
        })
        
        logger.info(f"Reset bucket '{bucket_id}' to capacity {capacity}")
        
        return {
            "status": "success",
            "bucket_id": bucket_id,
            "tokens": capacity
        }
