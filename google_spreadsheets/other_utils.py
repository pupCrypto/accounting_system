from functools import singledispatch


@singledispatch
def to_rgb(color):
    raise ValueError('Argument "color" must have view "#ffffff" or [255, 255, 255]')


@to_rgb.register
def _(color: str):
    value = color.lstrip('#').lower()
    lv = len(value)
    rgb = tuple(round(int(value[i:i + lv // 3], 16) / 255, 8) for i in range(0, lv, lv // 3))
    return {'red': rgb[0], 'green': rgb[1], 'blue': rgb[2]}


@to_rgb.register
def _(color: list):
    rgb = tuple(round(i / 255, 8) for i in color)
    return {'red': rgb[0], 'green': rgb[1], 'blue': rgb[2]}


@to_rgb.register
def _(color: tuple):
    rgb = tuple(round(i / 255, 8) for i in color)
    return {'red': rgb[0], 'green': rgb[1], 'blue': rgb[2]}


@to_rgb.register
def _(color: dict):
    return color