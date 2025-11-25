"""Document processing business logic."""
import logging
import os
import hashlib
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


class DocumentService:
    """Service for document processing operations.
    
    Provides methods for generating unique document identifiers and
    splitting documents into manageable chunks with optional overlap.
    """
    
    @staticmethod
    def generate_document_id(file_path: str, content: str) -> str:
        """Generate unique document ID based on file path and content hash.
        
        Args:
            file_path: Path to the document file.
            content: Document content.
            
        Returns:
            str: Unique document identifier combining filename and content hash.
        """
        content_hash = hashlib.sha256(content.encode('utf-8')).hexdigest()[:16]
        filename = os.path.basename(file_path)
        return f"{filename}_{content_hash}"
    
    @staticmethod
    def chunk_document(
        document_id: str,
        content: str,
        chunk_size: int,
        chunk_overlap: int
    ) -> List[Dict[str, Any]]:
        """Split document into overlapping chunks.
        
        Args:
            document_id: Unique identifier for the document.
            content: Document content to chunk.
            chunk_size: Size of each chunk in characters.
            chunk_overlap: Number of overlapping characters between chunks.
            
        Returns:
            List[Dict[str, Any]]: List of chunk dictionaries containing content,
                document_id, chunk_index, start_pos, and end_pos.
        """
        chunks = []
        
        for i in range(0, len(content), chunk_size - chunk_overlap):
            chunk_content = content[i:i + chunk_size]
            chunk = {
                "content": chunk_content,
                "document_id": document_id,
                "chunk_index": len(chunks),
                "start_pos": i,
                "end_pos": min(i + chunk_size, len(content))
            }
            chunks.append(chunk)
        
        logger.debug(f"Created {len(chunks)} chunks for document {document_id}")
        return chunks
