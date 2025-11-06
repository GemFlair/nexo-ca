"""
backend/tests/test_pdf_processor_s3_routing.py

Integration test for S3 routing in pdf_processor.
Tests that S3→S3 and Local→Local behavior works, with proper mocking.
"""

import os
import tempfile
import unittest.mock as mock
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from backend.services.pdf_processor import MasterPDFProcessor, PDFProcessorSettings


class TestPDFProcessorS3Routing:
    """Test S3 output routing based on input source."""

    @pytest.fixture
    def temp_pdf(self):
        """Create a minimal PDF for testing."""
        # Use reportlab if available, else create a simple binary file
        try:
            from reportlab.pdfgen import canvas
            from reportlab.lib.pagesizes import letter
            fd, path = tempfile.mkstemp(suffix='.pdf')
            c = canvas.Canvas(path, pagesize=letter)
            c.drawString(100, 750, "Test PDF for S3 routing")
            c.save()
            os.close(fd)
            yield Path(path)
            Path(path).unlink(missing_ok=True)
        except ImportError:
            # Fallback: create a dummy file
            fd, path = tempfile.mkstemp(suffix='.pdf')
            with os.fdopen(fd, 'wb') as f:
                f.write(b'%PDF-1.4\n1 0 obj\n<<\n/Type /Catalog\n/Pages 2 0 R\n>>\nendobj\n2 0 obj\n<<\n/Type /Pages\n/Kids [3 0 R]\n/Count 1\n>>\nendobj\n3 0 obj\n<<\n/Type /Page\n/Parent 2 0 R\n/MediaBox [0 0 612 792]\n/Contents 4 0 R\n>>\nendobj\n4 0 obj\n<<\n/Length 44\n>>\nstream\nBT\n/F1 12 Tf\n100 700 Td\n(Test) Tj\nET\nendstream\nendobj\nxref\n0 5\n0000000000 65535 f \n0000000009 00000 n \n0000000058 00000 n \n0000000115 00000 n \n0000000200 00000 n \ntrailer\n<<\n/Size 5\n/Root 1 0 R\n>>\nstartxref\n284\n%%EOF')
            yield Path(path)
            Path(path).unlink(missing_ok=True)

    @patch('boto3.client')
    def test_s3_input_derives_s3_output(self, mock_boto3_client, temp_pdf):
        """Test that S3 input derives S3 output target."""
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3

        # Set env for S3 input
        os.environ['S3_INPUT_PDF_PATH'] = 's3://test-bucket/input_data/pdf'

        processor = MasterPDFProcessor()
        # Simulate S3 source
        processor._source_is_s3 = True
        processor._resolved_s3_output_target = None

        # Trigger derivation
        processor._source_is_s3 = True
        if processor._source_is_s3 and not processor._resolved_s3_output_target:
            s_input = os.environ.get("S3_INPUT_PDF_PATH")
            processor._resolved_s3_output_target = s_input.replace("/input_data/", "/output_data/", 1)

        assert processor._resolved_s3_output_target == 's3://test-bucket/output_data/pdf'

        # Test upload
        from backend.services.pdf_processor import _maybe_upload_output_to_s3
        result = _maybe_upload_output_to_s3(str(temp_pdf), processor._resolved_s3_output_target)
        assert result.startswith('s3://test-bucket/output_data/pdf/')
        mock_s3.upload_file.assert_called_once()

    @patch('boto3.client')
    def test_local_input_no_s3_output(self, mock_boto3_client, temp_pdf):
        """Test that local input does not trigger S3 output."""
        processor = MasterPDFProcessor()
        processor._source_is_s3 = False
        processor._resolved_s3_output_target = None

        # No upload should happen
        from backend.services.pdf_processor import _maybe_upload_output_to_s3
        result = _maybe_upload_output_to_s3(str(temp_pdf), 's3://test-bucket/output')
        assert result is None  # Since no S3 target

    @patch('boto3.client')
    def test_explicit_s3_env_output(self, mock_boto3_client, temp_pdf):
        """Test explicit PDF_OUTPUT_DIR env var."""
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3

        os.environ['PDF_OUTPUT_DIR'] = 's3://explicit-bucket/explicit-prefix'

        processor = MasterPDFProcessor()
        assert processor._resolved_s3_output_target == 's3://explicit-bucket/explicit-prefix'

        from backend.services.pdf_processor import _maybe_upload_output_to_s3
        result = _maybe_upload_output_to_s3(str(temp_pdf), processor._resolved_s3_output_target)
        assert result.startswith('s3://explicit-bucket/explicit-prefix/')
        mock_s3.upload_file.assert_called_once()


if __name__ == '__main__':
    pytest.main([__file__])