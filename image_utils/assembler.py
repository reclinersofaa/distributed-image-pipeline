from PIL import Image


def assemble_image(tiles, grid_size, original_size, tile_size=(128, 128)):
    """
    Reassembles image from tiles.

    tiles: list of dicts with keys:
        - position (row, col)
        - image (PIL Image)
    """
    rows, cols = grid_size
    tile_w, tile_h = tile_size

    final_image = Image.new("RGB", original_size)

    for tile in tiles:
        row, col = tile["position"]
        x = col * tile_w
        y = row * tile_h

        final_image.paste(tile["image"], (x, y))

    return final_image
