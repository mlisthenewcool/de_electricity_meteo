## Todo

* .env file, should be removed and find a cleaner way to use absolute imports everywhere
  when running code with *uv*
    * https://github.com/direnv/direnv ?

* chose a visualization backend
    * Kepler.gl, pydeck, lonboard
    * https://github.com/visgl/deck.gl
    * https://github.com/developmentseed/lonboard

## Development

* [enable watch mode on Marimo](https://docs.marimo.io/guides/editor_features/watching/#watching-for-changes-to-your-notebook)

## Data sources

* electricity
    * installations (chosen)
        * https://odre.opendatasoft.com/explore/dataset/registre-national-installation-production-stockage-electricite-agrege/
    * general
        * https://analysesetdonnees.rte-france.com/

* geo
    * CONTOURS...IRIS (chosen, best if possible)
        * [source](https://geoservices.ign.fr/contoursiris)
        * [documentation](https://geoservices.ign.fr/documentation/donnees/vecteur/contoursiris)
    * ADMIN EXPRESS → COG (chosen, 2nd best if possible)
        * [source](https://geoservices.ign.fr/adminexpress) → updated once per month
        * [documentation](https://geoservices.ign.fr/documentation/donnees/vecteur/adminexpress)
    * might be util ?
        * https://github.com/InseeFrLab/pynsee