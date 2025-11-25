"""Document processing routes."""
import logging
from typing import List, Dict, Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from services.document_service import DocumentService

logger = logging.getLogger(__name__)

router = APIRouter()


# Pydantic models
class ChunkRequest(BaseModel):
    document_id: str
    content: str
    chunk_size: int
    chunk_overlap: int


class ChunkResponse(BaseModel):
    chunks: List[Dict[str, Any]]


class DocumentIdRequest(BaseModel):
    file_path: str
    content: str


# Initialize service
document_service = DocumentService()


@router.post("/generate-id", response_model=dict)
async def generate_id(request: DocumentIdRequest):
    """Generate document ID from file path and content.
    
    Args:
        request: Request containing file path and content.
        
    Returns:
        dict: Dictionary with generated document_id.
        
    Example:
        Request:
        ```json
        {
            "file_path": "/path/to/document.txt",
            "content": "Sample document content..."
        }
        ```
        
        Response:
        ```json
        {
            "document_id": "document.txt_a1b2c3d4e5f6g7h8"
        }
        ```
    """
    try:
        doc_id = document_service.generate_document_id(
            request.file_path,
            request.content
        )
        return {"document_id": doc_id}
    except Exception as e:
        logger.exception("Error generating document ID")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/chunk", response_model=ChunkResponse)
async def chunk_text(request: ChunkRequest):
    """Chunk document into overlapping segments.
    
    Args:
        request: Request containing document content and chunking parameters.
        
    Returns:
        ChunkResponse: Response with list of chunks.
        
    Example:
        Request:
        ```json
        {
            "document_id": "document.txt_a1b2c3d4",
            "content": "This is a long document...",
            "chunk_size": 1000,
            "chunk_overlap": 200
        }
        ```
        
        Response:
        ```json
        {
            "chunks": [
                {
                    "content": "This is a long document...",
                    "document_id": "document.txt_a1b2c3d4",
                    "chunk_index": 0,
                    "start_pos": 0,
                    "end_pos": 1000
                }
            ]
        }
        ```
    """
    try:
        chunks = document_service.chunk_document(
            request.document_id,
            request.content,
            request.chunk_size,
            request.chunk_overlap
        )
        
        logger.info(
            f"Chunked document {request.document_id} into {len(chunks)} chunks"
        )
        return ChunkResponse(chunks=chunks)
        
    except Exception as e:
        logger.exception("Error chunking document")
        raise HTTPException(status_code=500, detail=str(e))
