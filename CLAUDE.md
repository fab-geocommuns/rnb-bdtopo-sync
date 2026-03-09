- always use the virtual environnement, located in .venv/, to run tests and scripts
- tests use unittest
- test data should be the size of an house or warehouse. no test data the size of a city or greater
- test data must be expressed in geojson with WGS84 coordinates
- when asked to display test data in geojson, create a feature collection. For each feature add its layer (rnb or bdtopo), stroke and fill-stroke properties (#1c5cf2 if rnb, #63ef06 if bdtopo). display the geojson so it can be copy/pasted
  s
