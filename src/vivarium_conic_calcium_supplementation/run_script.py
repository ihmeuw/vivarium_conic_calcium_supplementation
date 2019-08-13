from vivarium.interface import setup_simulation_from_model_specification

sim = setup_simulation_from_model_specification('./model_specifications/vivarium_conic_calcium_supplementation_mali.yaml')
sim.run()
