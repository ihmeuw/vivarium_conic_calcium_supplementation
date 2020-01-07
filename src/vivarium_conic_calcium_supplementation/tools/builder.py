import itertools
from pathlib import Path
from typing import Sequence

from loguru import logger

from vivarium.framework.artifact import EntityKey, get_location_term, Artifact
from vivarium_inputs.data_artifact.loaders import loader


def create_new_artifact(path: str, location: str) -> Artifact:
    logger.info(f"Creating artifact at {path}.")
    artifact = Artifact(path, filter_terms=[get_location_term(location)])
    key = EntityKey('metadata.locations')
    if str(key) not in artifact:
        logger.info(f'\tWriting {key}.')
        artifact.write('metadata.locations', [location])
    else:
        logger.info(f'\t{key} found in artifact.')
    return artifact


def safe_write(artifact: Artifact, keys: Sequence, location: str):
    for key in keys:
        if str(key) not in artifact:
            logger.info(f'\tWriting {key}.')
            data = loader(key, location, set())
            artifact.write(key, data)
        else:
            logger.info(f'\t{key} found in artifact.')


def write_demographic_data(artifact: Artifact, location: str):
    logger.info('Writing demographic data...')

    keys = [EntityKey('population.structure'),
            EntityKey('population.age_bins'),
            EntityKey('population.theoretical_minimum_risk_life_expectancy'),
            EntityKey('population.demographic_dimensions')]
    safe_write(artifact, keys, location)


def write_covariate_data(artifact: Artifact, location: str):
    logger.info('Writing covariate data...')

    covariates = ['live_births_by_sex', 'antenatal_care_1_visit_coverage_proporiton']
    measures = ['estimate']

    keys = [EntityKey(f'covariate.{c}.{m}') for c, m in itertools.product(covariates, measures)]
    safe_write(artifact, keys, location)


def write_disease_data(artifact: Artifact, location: str):
    logger.info('Writing disease data...')

    cause_measures = {
        'all_causes': ['cause_specific_mortality_rate'],
        'diarrheal_diseases':
            ['cause_specific_mortality_rate', 'excess_mortality_rate', 'disability_weight',
             'incidence_rate', 'prevalence', 'remission'],
        'lower_respiratory_infections':
            ['cause_specific_mortality_rate', 'excess_mortality_rate', 'disability_weight'
             'incidence_rate', 'prevalence', 'remission'],
        'measles':
            ['cause_specific_mortality_rate', 'excess_mortality_rate', 'disability_weight',
             'incidence_rate', 'prevalence'],
        'neonatal_sepsis_and_other_neonatal_infections':
            ['cause_specific_mortality_rate', 'excess_mortality_rate', 'disability_weight'
             'birth_prevalence', 'prevalence'],
        'neonatal_encephalopathy_due_to_birth_asphyxia_and_trauma':
            ['cause_specific_mortality_rate', 'excess_mortality_rate', 'disability_weight',
             'birth_prevalence', 'prevalence'],
        'neonatal_preterm_birth':
            ['cause_specific_mortality_rate', 'excess_mortality_rate', 'disability_weight'],
    }

    for cause, measures in cause_measures.items():
        keys = [EntityKey(f'cause.{cause}.{m}') for m in measures]
        safe_write(artifact, keys, location)


def write_alternative_risk_data(artifact, location):
    logger.info('Writing risk data...')

    risks = ['child_wasting', 'child_underweight', 'child_stunting']
    alternative_measures = ['exposure', 'exposure_distribution_weights', 'exposure_standard_deviation']
    keys = [EntityKey(f'alternative_risk_factor.{r}.{m}') for r, m in itertools.product(risks, alternative_measures)]
    safe_write(artifact, keys, location)

    measures = ['relative_risk', 'population_attributable_fraction']
    keys = [EntityKey(f'risk_factor.{r}.{m}') for r, m in itertools.product(risks, measures)]
    safe_write(artifact, keys, location)


def write_lbwsg_data(artifact, location):
    risk = 'low_birth_weight_and_short_gestation'
    measures = ['exposure', 'population_attributable_fraction', 'relative_risk']
    keys = [EntityKey(f'risk_factor.{risk}.{m}') for m in measures]

    # locations whose data was saved with an incompatible tables version
    if location in ['Mali']:
        data_source = Path('/share/costeffectiveness/lbwsg/artifacts') / location.replace(" ", "_")
        reversioned_artifact = Artifact(data_source)
        for key in keys:
            if str(key) not in artifact:
                logger.info(f'\tWriting {key}.')
                data = reversioned_artifact.load(key)
                artifact.write(key, data)
            else:
                logger.info(f'\t{key} found in artifact.')
    else:
        safe_write(artifact, keys, location)


def build_artifact(location: str, output_dir: str, erase: bool):

    artifact_path = Path(output_dir) / f'{location.replace(" ", "_")}.hdf'
    if erase and artifact_path.is_file():
        artifact_path.unlink()
    artifact = create_new_artifact(artifact_path, location)
    write_demographic_data(artifact, location)
    write_disease_data(artifact, location)
    write_alternative_risk_data(artifact, location)
    write_lbwsg_data(artifact, location)

    logger.info('!!! Done !!!')
