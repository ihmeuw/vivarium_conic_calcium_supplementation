# Data for some African countries, including Mali, Sudan, and the Central 
# African Republic, appears to have been changed on June 4th, 2019. The
# data was saved with a pandas+tables combination that is incompatible 
# with the plurality of data from GBD round 5. This means an environment
# can't be constructed that can pull all the data at once. 

# The strategy to build artifacts for this project, then, is to build the 
# artifact with the standard (expected versioning) with everything that
# works. Then, to create a new environment with patch-requires.txt in root,
# and run this patching script to add the new data.

# The data that does not work is low birth weight short gestation. It must 
# be commented out to build the base artifact.

from pathlib import Path

from vivarium_public_health import EntityKey
from vivarium_public_health.dataset_manager import Artifact
from vivarium_inputs.data_artifact.builder import _worker


def patch(art_path: str):
    """Patch a calcium supplementation artifact located at `art_path`
    with all the data that was saved with incompatible compression.
    
    This requires tables>3.4
    """
    
    artifact = Artifact(art_path)
    location = artifact.load("metadata.locations")[0]
    
    missing_keys = [
        EntityKey(risk_factor.low_birth_weight_and_short_gestation.categories),
        EntityKey(risk_factor.low_birth_weight_and_short_gestation.exposure),
        EntityKey(risk_factor.low_birth_weight_and_short_gestation.distribution),
        EntityKey(risk_factor.low_birth_weight_and_short_gestation.relative_risk),
        EntityKey(risk_factor.low_birth_weight_and_short_gestation.population_attributable_fraction)
    ]

    modeled_causes = ['diarrheal_diseases', 'lower_respiratory_infections', 'measles', 'neonatal_sepsis_and_other_neonatal_infections',
                      'neonatal_encephalopathy_due_to_birth_asphyxia_and_trauma', 'hemolytic_disease_and_other_neonatal_jaundice',
                      'neonatal_preterm']

    for ek in missing_keys:
        _worker(ek, location, modeled_causes, art)  # this is how build_artifacts does it
    

if __name__=="__main__":
    import sys
    path = Path(sys.argv[1])  # Path to an artifact file
    if (not path.is_file() ) or (path.suffix != '.hdf'):
        raise IllegalArgumentException() 
    patch(str(path))

