#from distutils.core import setup, Extension
from setuptools import setup, Extension
from distutils.sysconfig import get_python_inc
from Cython.Build import cythonize

import os

root = os.path.abspath(os.path.dirname(__file__))
ext_modules = []

include_dirs = [
    get_python_inc(plat_specific=True),
    os.path.join(root, 'include')
]


#define_macros=[('CYTHON_TRACE', '1')]
#extensions = [
#    Extension("lcm", ["lcm.pyx"], extra_compile_args=['-fPIC'], extra_link_args=['-fPIC'])
#]


extensions = [
    Extension("try_extern_cimport", ["try_extern_cimport.pyx"], extra_compile_args=['-fPIC'], extra_link_args=['-fPIC'])
]

setup(
    ext_modules=cythonize(extensions),
    install_requires=['roaringbitmap']
)



