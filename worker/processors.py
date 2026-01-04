import cv2
import numpy as np
from PIL import Image


def pil_to_cv(image: Image.Image):
    return cv2.cvtColor(np.array(image), cv2.COLOR_RGB2BGR)


def cv_to_pil(image):
    return Image.fromarray(cv2.cvtColor(image, cv2.COLOR_BGR2RGB))


def grayscale(image: Image.Image) -> Image.Image:
    cv_img = pil_to_cv(image)
    gray = cv2.cvtColor(cv_img, cv2.COLOR_BGR2GRAY)
    return Image.fromarray(gray)


def blur(image: Image.Image) -> Image.Image:
    cv_img = pil_to_cv(image)
    blurred = cv2.GaussianBlur(cv_img, (5, 5), 0)
    return cv_to_pil(blurred)


def edge_detect(image: Image.Image) -> Image.Image:
    cv_img = pil_to_cv(image)
    edges = cv2.Canny(cv_img, 100, 200)
    return Image.fromarray(edges)

def process_image(image, operation):
    """
    Dispatch image processing based on operation name.
    """
    if operation == "grayscale":
        return grayscale(image)
    elif operation == "blur":
        return blur(image)
    elif operation == "edge":
        return edge_detect(image)
    else:
        raise ValueError(f"Unknown operation: {operation}")