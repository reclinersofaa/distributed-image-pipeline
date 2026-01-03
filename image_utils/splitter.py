from PIL import Image
import math


def split_image(image_path, tile_size=(128, 128)):
    """
    Splits an image into tiles.

    Returns:
        tiles: list of dicts with keys:
            - tile_id
            - position (row, col)
            - image (PIL Image)
    """
    image = Image.open(image_path)
    width, height = image.size
    tile_w, tile_h = tile_size

    tiles = []
    tile_id = 0

    rows = math.ceil(height / tile_h)
    cols = math.ceil(width / tile_w)

    for row in range(rows):
        for col in range(cols):
            left = col * tile_w
            upper = row * tile_h
            right = min(left + tile_w, width)
            lower = min(upper + tile_h, height)

            tile = image.crop((left, upper, right, lower))

            tiles.append({
                "tile_id": tile_id,
                "position": (row, col),
                "image": tile
            })

            tile_id += 1

    return tiles, (rows, cols), image.size
