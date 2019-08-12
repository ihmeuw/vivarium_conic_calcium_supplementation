import pandas as pd
import numpy as np
import scipy.stats


class CalciumSupplementationIntervention:

    configuration_defaults = {
        'calcium_supplementation_intervention': {
            'proportion': 0.5,
            'birth_weight_shift': {   # grams
                'population': {
                    'mean': 0.5,
                    'sd': 0.25
                },
                'individual': {
                    'sd': 0.25
                }
            },
            'gestation_time_shift': {  # weeks
                'population': {
                    'mean': 0.5,
                    'sd': 0.25
                },
                'individual': {
                    'sd': 0.25
                }
            },
            'stunting_shift': 0,  # z-score
            'wasting_shift': 0,  # z-score
            'underweight_shift': 0,  # z-score
        }
    }

    def __init__(self):
        self.name = 'calcium_supplementation_intervention'

    def setup(self, builder):
        self.start_time = pd.Timestamp(**builder.configuration.time.start.to_dict())
        self.config = builder.configuration['calcium_supplementation_intervention']

        validate_configuration(self.config.to_dict())

        self.enrollment_randomness = builder.randomness.get_stream('calcium_supplementation_intervention_enrollment')
        self.effect_randomness = builder.randomness.get_stream('effect_draw')

        columns_created = ['calcium_supplementation_treatment_status']
        self.population_view = builder.population.get_view(columns_created)

        builder.population.initializes_simulants(self.on_initialize_simulants,
                                                 creates_columns=columns_created)

        builder.value.register_value_modifier('low_birth_weight_and_short_gestation.raw_exposure',
                                              self.adjust_lbwsg)
        builder.value.register_value_modifier('child_stunting.exposure',
                                              self.adjust_stunting)
        builder.value.register_value_modifier('child_wasting.exposure',
                                              self.adjust_wasting)
        builder.value.register_value_modifier('child_underweight.exposure',
                                              self.adjust_underweight)

        self.pop_birth_weight_mean = self.get_effect_size(self.config.birth_weight_shift.population.mean,
                                                          self.config.birth_weight_shift.population.sd,
                                                          'population_birth_weight')
        self.pop_gestation_time_mean = self.get_effect_size(self.config.gestation_time_shift.population.mean,
                                                            self.config.gestation_time_shift.population.sd,
                                                            'population_gestation_time')
        self.ind_birth_weight_effect = pd.Series()
        self.ind_gestation_time_effect = pd.Series()

    def on_initialize_simulants(self, pop_data):
        pop = pd.DataFrame({'calcium_supplementation_treatment_status': 'not_treated'}, index=pop_data.index)
        if pop_data.creation_time > self.start_time:
            treatment_probability = self.config.proportion
            treated = self.enrollment_randomness.filter_for_probability(pop.index, treatment_probability)
            pop.loc[treated, 'calcium_supplementation_treatment_status'] = 'treated'

            ind_birth_effect = self.get_effect_size(self.pop_birth_weight_mean,
                                                    self.config.birth_weight_shift.individual.sd,
                                                    'individual_birth_weight')
            self.ind_birth_weight_effect = self.ind_birth_weight_effect.append(ind_birth_effect, index=pop_data.index)

            ind_gestation_effect = self.get_effect_size(self.pop_gestation_time_mean,
                                                        self.config.gestation_time_shift.individual.sd,
                                                        'individual_gestation_time')
            self.ind_gestation_time_effect = self.ind_gestation_time_effect.append(ind_gestation_effect, index=pop_data.index)

        self.population_view.update(pop)

    def get_effect_size(self, mean, sd, key):
        r = np.random.RandomState(self.effect_randomness.get_seed(additional_key=key))
        draw = r.uniform()
        effect = scipy.stats.norm(mean, sd).ppf(draw)
        return effect

    def adjust_lbwsg(self, index, exposure):
        pop = self.population_view.get(index)
        exposure['birth_weight'] += self.config.birth_weight_shift * (pop.calcium_supplementation_treatment_status == 'treated')
        exposure['gestation_time'] += self.config.gestation_time_shift * (pop.calcium_supplementation_treatment_status == 'treated')
        return exposure

    def adjust_stunting(self, index, exposure):
        pop = self.population_view.get(index)
        return exposure + self.config.stunting_shift * (pop.calcium_supplementation_treatment_status == 'treated')

    def adjust_wasting(self, index, exposure):
        pop = self.population_view.get(index)
        return exposure + self.config.wasting_shift * (pop.calcium_supplementation_treatment_status == 'treated')

    def adjust_underweight(self, index, exposure):
        pop = self.population_view.get(index)
        return exposure + self.config.underweight_shift * (pop.calcium_supplementation_treatment_status == 'treated')


def validate_configuration(config):
    if not (0 <= config['proportion'] <= 1):
        raise ValueError(f'The proportion for calcium supplementation intervention must be between 0 and 1.'
                         f'You specified {config.proportion}.')
    for key in config:
        if 'shift' in key and isinstance(config[key], int) and config[key] < 0:
            raise ValueError(f'Additive shift for {key} must be positive.')

    # TODO: Test the nested dictionaries

