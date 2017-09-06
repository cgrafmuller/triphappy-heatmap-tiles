# coding=utf-8
# Wrapper for lucasb-eyer heatmap C library found here: https://github.com/lucasb-eyer/heatmap
# Uses the ctypes library to interact with the C module
# Made with love by Carl Grafmuller 😘 (https://github.com/cgrafmuller)

from os.path import join as pjoin, dirname
from ctypes import CDLL, c_ulong, c_float, c_ubyte, c_size_t, POINTER, Structure
from PIL import Image


# Some of the C functions return a pointer to a C struct - here the C structures are defined in Python
class heatmap_t(Structure):
    _fields_ = [
        ("buf", POINTER(c_float)),
        ("max", c_float),
        ("w", c_ulong),
        ("h", c_ulong)
    ]


class heatmap_stamp_t(Structure):
    _fields_ = [
        ("buf", POINTER(c_float)),
        ("w", c_ulong),
        ("h", c_ulong)
    ]


class heatmap_colorscheme_t(Structure):
    _fields_ = [
        ("colors", POINTER(c_ubyte)),
        ("ncolors", c_size_t)
    ]


class Heatmap:
    def __init__(self):

        # NOTE: the C library must be compiled in shared mode to generate the external file
        # The libHeatmap.so file must be located in the same directory as this file
        # Will NOT work in Windows (sorry, Ben)
        #
        # To-do: write logic to load in Windows .dll files if they exist
        #        write logic to search for the file in other locations
        self.libhm = CDLL(pjoin(dirname(__file__), 'libHeatmap.so'))

        if not self.libhm:
            raise Exception("Heatmap shared library not found in directory.")

        # Define the return types of these C functions as pointers to the structures defined above
        self.libhm.heatmap_new.restype = POINTER(heatmap_t)
        self.libhm.heatmap_stamp_gen.restype = POINTER(heatmap_stamp_t)
        self.libhm.heatmap_colorscheme_load.restype = POINTER(heatmap_colorscheme_t)

    # All of the default color schemes have alpha (opacity) levels of 255 -> meaning no transparency
    # This function will go through every pixel of the heatmap & multiply the alpha value by the requested opacity
    # Opacity should be input as a float between 0 and 1
    # An opacity of 1 will do nothing, an opacity of 0 will turn the WHOLE image transparent
    # Function runs a bit slow, a different option is to use a custom color scheme with defined alpha values
    def set_opacity(self, img, opacity):
        for x in range(0, img.width):
            for y in range(0, img.height):
                rgba = img.getpixel((x, y))
                if rgba[3] > 0:
                    new_rgba = (rgba[0], rgba[1], rgba[2], int(rgba[3] * opacity))
                    img.putpixel((x, y), new_rgba)

    # Generates a heatmap with the requested points
    # Points should be normalized before calling this function (i.e. point values should be within the size params)
    # Dotsize is the pixel radius of the "stamp" that the heatmap uses
    # Opacity is an optional parameter to adjust the entire opacity of the resulting heatmap. Should be > 0 and < 1
    # Size is the pixel size of the heatmap. The point data should be normalized to these bounds.
    # Custom color schemes can be defined as well.
    def heatmap(self, points, dotsize=150, opacity=1, size=(1024, 1024), color_scheme=None):
        width = size[0]
        height = size[1]

        # Create the heatmap object with the given dimensions (in pixels)
        hm = self.libhm.heatmap_new(width, height)

        # Generate a linear-distribution, circular stamp with the dotsize as the radius
        stamp = self.libhm.heatmap_stamp_gen(c_ulong(dotsize))

        # Add the points to the heatmap!
        # NOTE: the C module only accepts integer values
        #
        # To-do: Re-write C function to take in floats?
        for pt in points:
            pt = (int(pt[0]), int(pt[1]))
            self.libhm.heatmap_add_point_with_stamp(hm, c_ulong(pt[0]), c_ulong(pt[1]), stamp)

        # This creates an image out of the heatmap with the requested color scheme
        # If no color scheme is provided, use the default scheme
        # `rawimg` now contains the image data in 32-bit RGBA
        rawimg = (c_ubyte * (width * height * 4))()
        if color_scheme is None:
            self.libhm.heatmap_render_default_to(hm, rawimg)
        else:
            self.libhm.heatmap_render_to(hm, color_scheme, rawimg)

        # Free up the memory allocated to the C objects
        self.libhm.heatmap_free(hm)
        self.libhm.heatmap_stamp_free(stamp)

        # Generate a PNG file from the raw image data
        # Set opacity if requested
        img = Image.frombuffer('RGBA', (width, height), rawimg, 'raw', 'RGBA', 0, 1)
        if opacity != 1:
            self.set_opacity(img, opacity)

        return img
