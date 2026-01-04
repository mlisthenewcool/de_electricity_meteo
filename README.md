## Todo

* .env file, should be removed and find a cleaner way to use absolute imports everywhere
  when running code with *uv*
    * https://github.com/direnv/direnv ?

* chose a visualization backend
    * Kepler.gl, pydeck, lonboard
    * https://github.com/visgl/deck.gl
    * https://github.com/developmentseed/lonboard

## Development workflow

* This project uses :
    * ruff (check & format)
    * ty (type check)
    * pre-commit (pre-commit hooks)

Workflow to commit using pre-commit hooks:

```shell
uv sync --upgrade # --all-groups
git add .
uv run pre-commit run
# now, you should correct errors in the above command
# ...
# then execute the following
git add . && git commit "message"

# if somehow you forgot to run pre-commit before commiting
# use the following command to avoid polluting the git history 
git add . && git commit --amend --no-edit
```

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