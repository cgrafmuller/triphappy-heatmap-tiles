# triphappy-heatmap-tiles
Generates heatmap tiles for use in Google Maps. As used by triphappy.com

Uses the wonderful [Heatmap](https://github.com/lucasb-eyer/heatmap) library by Lucas B Eyer

## Getting Started

* Designed to be used with Amazon RDS to hold all the heatmap point information & Amazon S3 to host the generated tiles
* Change the required information within rds_config.py accordingly
* Maybe recompile heatmap.c?
* Enjoy

## Compiling heatmap.c

* You might need to recompile heatmaps/heatmap_c/heatmap.c for your own machine into a new version of libHeatmap.so
* If on OS X, try "gcc -Wall -Wextra -O -ansi -pedantic -shared heatmap.c -o libHeatmap.so"
* If on Linux, try "gcc -c -Wall -Werror -fpic heatmap.c", followed by "gcc -shared -o libHeatmap.so heatmap.o"
