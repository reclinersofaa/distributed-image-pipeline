import base64
import io
from PIL import Image


def image_to_base64(image: Image.Image) -> str:
    """
    Converts PIL Image to Base64 string.
    """
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    encoded = base64.b64encode(buffer.getvalue()).decode("utf-8")
    return encoded


def base64_to_image(encoded_str: str) -> Image.Image:
    """
    Converts Base64 string back to PIL Image.
    """
    image_bytes = base64.b64decode(encoded_str)
    buffer = io.BytesIO(image_bytes)
    return Image.open(buffer)
