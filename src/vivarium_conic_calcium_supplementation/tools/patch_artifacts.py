# Low birthweight and short gestation data for some African countries, 
# including Mali, Sudan, and the Central African Republic, appears to have
# been changed on June 4th, 2019. The data was saved with a pandas+tables
# combination that is incompatible with the plurality of data from GBD
# round 5. This means an environment can't be constructed that can pull all
# the data at once. 

# Additionally, this only applies to some of the low birthweight and short
# gestation data. However, when the component is added to a model
# specification, the artifact builder will try to pull all the data it
# needs -- some of which requires tables 3.4 and some greater than 3.4. Also,
# neonatal preterm is directly dependent on the existence of low birthweight
# and short gestation data so it cannot be in the model specification without
# it.

# So, the strategy to build artifacts for this project, is to build a base
# artifact with tables 3.4 for all data excluding low birthweight and short
# gestation and neonatal preterm (comment them out). That data must then be
# patched in using this script, once with an environment with tables 3.4 and
# another with greater than. In the root of this repository, 
# patch-requires.txt defines a patching environment.


from pathlib import Path

from vivarium_public_health.dataset_manager import Artifact, EntityKey, ArtifactException
from vivarium_inputs.data_artifact.builder import _worker


def patch(art_path: str, ver: str='3.4'):
    """Patch a calcium supplementation artifact located at `art_path`
    with all the data that was saved with incompatible compression.
    
    This requires tables>3.4
    """
    
    artifact = Artifact(art_path)
    location = artifact.load("metadata.locations")[0]
    
    # low birthweight short gestation is split between 3.4 and >3.4
    if ver == '3.4':
        missing_keys = [
            EntityKey("risk_factor.low_birth_weight_and_short_gestation.relative_risk"),
            EntityKey("risk_factor.low_birth_weight_and_short_gestation.population_attributable_fraction"),
            EntityKey("cause.neonatal_preterm_birth.cause_specific_mortality"),
            EntityKey("cause.neonatal_preterm_birth.disability_weight"),
            EntityKey("cause.neonatal_preterm_birth.excess_mortality")
        ]
    else:
        missing_keys = [
            EntityKey("risk_factor.low_birth_weight_and_short_gestation.categories"),
            EntityKey("risk_factor.low_birth_weight_and_short_gestation.exposure"),
            EntityKey("risk_factor.low_birth_weight_and_short_gestation.distribution")
        ]


    modeled_causes = ['diarrheal_diseases', 'lower_respiratory_infections', 'measles', 'neonatal_sepsis_and_other_neonatal_infections',
                      'neonatal_encephalopathy_due_to_birth_asphyxia_and_trauma', 'hemolytic_disease_and_other_neonatal_jaundice',
                      'neonatal_preterm']

    for ek in missing_keys:
        print(f"Patching {ek}")
        try:
            _worker(ek, location, modeled_causes, artifact)  # this is how build_artifacts does it
        except ArtifactException as e:
            print(e)  # ___ already in artifact


if __name__=="__main__":
    import sys
    path = Path(sys.argv[1])  # Path to an artifact file
    if (not path.is_file() ) or (path.suffix != '.hdf'):
        raise ValueError("First argument must be an hdf file Artifact")
    version = sys.argv[2]
    if version not in ['3.4', '3.5']:
        raise ValueError("Second 'tables version' argument must be one of {3.4, 3.5}")
    patch(str(path), version)

