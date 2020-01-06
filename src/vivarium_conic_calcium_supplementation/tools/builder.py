import itertools
from pathlib import Path
from typing import Sequence

from loguru import logger

from vivarium.framework.artifact import EntityKey, get_location_term, Artifact
from vivarium_inputs.data_artifact.loaders import loader


def create_new_artifact(path: str, location: str) -> Artifact:
    logger.info(f"Creating artifact at {path}.")
    art = Artifact(path, filter_terms=[get_location_term(location)])
    key = EntityKey('metadata.locations')
    if str(key) not in art:
        logger.info(f'\tWriting {key}.')
        art.write('metadata.locations', [location])
    else:
        logger.info(f'\t{key} found in artifact.')
    return art


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
        'all_causes': ['cause_specific_mortality'],
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

    for causes, measures in cause_measures.items():
        keys = [EntityKey(f'cause.{c}.{m}') for c, m in itertools.product(causes, measures)]
        safe_write(artifact, keys, location)


def write_risk_data():
    pass


def build_artifact(location: str, output_dir: str, erase: bool):

    artifact_path = Path(output_dir) / f'{location.replace(" ", "_").lower()}.hdf'
    if erase and artifact_path.is_file():
        artifact_path.unlink()
    artifact = create_new_artifact(artifact_path, location)
    write_demographic_data(artifact, location)
    write_disease_data(artifact, location)
    write_risk_data()

    logger.info('!!! Done !!!')
