## Todo

* .env file, should be removed and find a cleaner way to use absolute imports everywhere
  when running code with *uv*
    * https://github.com/direnv/direnv ?

* chose a visualization backend
    * Kepler.gl, pydeck, lonboard
    * https://github.com/visgl/deck.gl
    * https://github.com/developmentseed/lonboard

* potentially useful libraries
    * inspirations ?
        * https://github.com/CharlieSergeant/airflow-minio-postgres-fastapi/tree/main
        * https://www.electricitymaps.com/data/methodology
        * https://app.electricitymaps.com/coverage?q=fr
    * ETL/ELT: https://github.com/airbytehq/airbyte
    * data git : https://github.com/treeverse/dvc
    * deploy
        * https://github.com/dokploy/dokploy
        * https://github.com/coollabsio/coolify
    * observability
        * https://www.netdata.cloud/
        * grafana (+) prometheus

## Development workflow

Basic commands to keep uv & python up-to-date:

```shell
uv self update
uv python upgrade
```

* This project uses :
    * ruff (lint & format)
        * `uv run ruff check --show-files`
        * `uv run ruff format --verbose`
    * ty (type check)
        * `uv run ty check --verbose`
    * pytest (tests + code coverage)
        * `uv run pytest --verbose`
    * pre-commit (pre-commit hooks)
        * `uv run pre-commit run --verbose`

Workflow to commit using pre-commit hooks:

```shell
uv sync --upgrade # --all-groups
git add .
uv run pre-commit run --verbose
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

* Electricity
    * https://odre.opendatasoft.com/explore/dataset/registre-national-installation-production-stockage-electricite-agrege/
        * documentation and exports (parquet is preferred) available on the upper url
        * batch integration: updated once per year but there is no fixed date, the url should always point to the
          latest available data
        * data quality/enhancement
            * check with known datasets for annual production of different scales (sector, region, general)
                * https://analysesetdonnees.rte-france.com/
                * https://www.services-rte.com/fr/visualisez-les-donnees-publiees-par-rte/capacite-installee-de-production.html
    * https://odre.opendatasoft.com/explore/dataset/eco2mix-regional-cons-def
        * todo documentation
    * https://odre.opendatasoft.com/explore/dataset/eco2mix-regional-tr
        * todo documentation
        * certaines données ne sont pas remontées immédiatement (vérifier, mais environ 1h).

* Météo
    * https://www.data.gouv.fr/datasets/informations-sur-les-stations-metadonnees
        * liste des stations avec historique complet

* Geo
    * CONTOURS...IRIS (chosen, best if possible)
        * [source](https://geoservices.ign.fr/contoursiris)
        * [documentation](https://geoservices.ign.fr/documentation/donnees/vecteur/contoursiris)
        * todo: is IRIS GE better for the project ?
    * ADMIN EXPRESS → COG (2nd best if possible)
        * [source](https://geoservices.ign.fr/adminexpress) → updated once per month
        * [documentation](https://geoservices.ign.fr/documentation/donnees/vecteur/adminexpress)
    * might be util ?
        * https://github.com/InseeFrLab/pynsee

# AI

* code review: https://www.coderabbit.ai/

### Todo

* énergie
    * https://data.rte-france.com/
        * problème d'inscription
    * https://www.data.gouv.fr/pages/donnees-energie/
        * page générale pour les données de production/consommation d'énergie (pas uniquement électrique)
    * https://www.data.gouv.fr/datasets/generation-forecast
        * prévisionnel J+3 J+2 J+1 si besoin ?

    * données production/consommation régionales
        * (temps
          réel) https://odre.opendatasoft.com/explore/dataset/eco2mix-regional-tr/information/?disjunctive.nature&disjunctive.libelle_region
            * (explications) https://www.rte-france.com/donnees-publications/eco2mix-donnees-temps-reel
        * (consolidées &
          définitives) https://odre.opendatasoft.com/explore/dataset/eco2mix-regional-cons-def/information/?disjunctive.nature&disjunctive.libelle_region

* géographique
    * relier commune/iris/epci/... (trouver la maille la plus fine accessible)
        * https://public.opendatasoft.com/explore/assets/georef-france-commune-arrondissement-municipal/
        * https://public.opendatasoft.com/explore/assets/georef-france-commune-millesime/
        * https://www.data.gouv.fr/datasets/referentiel-geographique-francais-communes-unites-urbaines-aires-urbaines-departements-academies-regions
        * https://www.insee.fr/fr/information/7708995
        * https://www.data.gouv.fr/datasets/admin-express-admin-express-cog-admin-express-cog-carto-admin-express-cog-carto-pe-admin-express-cog-carto-plus-pe
        * www.data.gouv.fr/datasets/iris-ge
        * https://gitlab.com/Oslandia/pyris

* météorologiques
    * (base des deux API disponible) https://donneespubliques.meteofrance.fr/
        * exploration https://github.com/loicduffar/meteo.data-Tools
        * https://www.data.gouv.fr/datasets/donnees-climatologiques-de-base-horaires
            * (identique normalement) https://meteo.data.gouv.fr/datasets/6569b4473bedf2e7abad3b72
        * https://www.data.gouv.fr/datasets/liste-des-stations-en-open-data-du-reseau-meteorologique-infoclimat-static-et-meteo-france-synop
            * pas de Météo France
        * https://www.infoclimat.fr/opendata/stations_xhr.php?format=geojson
            * liste

    * data quality des données
        * https://confluence-meteofrance.atlassian.net/wiki/spaces/OpenDataMeteoFrance/pages/621510657/Donn+es+climatologiques+de+base
            * par exemple, les stations météo de type 0 sont les plus précises, y a-t-il une différence flagrante de
              précision avec les autres et est-ce que ça impacte la qualité de l'analyse ?
