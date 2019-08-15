from datetime import datetime

import pandas as pd


class SampleHistoryObserver:

    configuration_defaults = {
        'metrics': {
            'sample_history_observer': {
                'sample_size': 1000,
                'path': f'/share/costeffectiveness/results/vivarium_conic_calcium_supplementation/sample_history.hdf'
            }
        }
    }

    @property
    def name(self):
        return "sample_history_observer"

    def __init__(self):
        self.history_snapshots = []
        self.sample_index = None

    def setup(self, builder):
        self.clock = builder.time.clock()
        self.sample_history_parameters = builder.configuration.metrics.bmi_sample_history_observer
        self.randomness = builder.randomness.get_stream("sample_history")

        # sets the sample index
        builder.population.initializes_simulants(self.get_sample_index)

        columns_required = ['alive', 'age', 'sex', 'exit_time',
                            'years_lived_with_disability',
                            'years_of_life_lost',
                            'treatment_start', 'treatment_end',
                            'healthcare_last_visit_date',
                            'ischemic_heart_disease_event_time',
                            'ischemic_stroke_event_time',
                            'diabetes_mellitus_type_2_event_time',
                            'gout_event_time',
                            'asthma_event_time',
                            'chronic_kidney_disease_due_to_hypertension_event_time',
                            'chronic_kidney_disease_due_to_glomerulonephritis_event_time',
                            'chronic_kidney_disease_due_to_other_and_unspecified_causes_event_time',
                            'chronic_kidney_disease_due_to_diabetes_mellitus_type_2_event_time']
        self.population_view = builder.population.get_view(columns_required)

        # keys will become column names in the output
        self.pipelines = {'bmi_exposure': builder.value.get_value('high_body_mass_index_in_adults.exposure'),
                          'disability_weight': builder.value.get_value('disability_weight'),
                          'mortality_rate': builder.value.get_value('mortality_rate'),
                          'ischemic_heart_disease_incidence': builder.value.get_value('ischemic_heart_disease.incidence_rate'),
                          'ischemic_stroke_incidence': builder.value.get_value('ischemic_stroke.incidence_rate'),
                          'diabetes_mellitus_type_2_incidence': builder.value.get_value('diabetes_mellitus_type_2.incidence_rate'),
                          'gout_incidence': builder.value.get_value('gout.incidence_rate'),
                          'asthma_incidence': builder.value.get_value('asthma.incidence_rate'),
                          'chronic_kidney_disease_due_to_hypertension_incidence': builder.value.get_value('chronic_kidney_disease_due_to_hypertension.incidence_rate'),
                          'chronic_kidney_disease_due_to_glomerulonephritis_incidence': builder.value.get_value('chronic_kidney_disease_due_to_glomerulonephritis.incidence_rate'),
                          'chronic_kidney_disease_due_to_other_and_unspecified_causes_incidence': builder.value.get_value('chronic_kidney_disease_due_to_other_and_unspecified_causes.incidence_rate'),
                          'chronic_kidney_disease_due_to_diabetes_mellitus_type_2_incidence': builder.value.get_value('chronic_kidney_disease_due_to_diabetes_mellitus_type_2.incidence_rate')}

        builder.event.register_listener('collect_metrics', self.record)
        builder.event.register_listener('simulation_end', self.dump)

        self.builder = builder

    def get_sample_index(self, pop_data):
        sample_size = self.sample_history_parameters.sample_size
        if sample_size is None or sample_size > len(pop_data.index):
            sample_size = len(pop_data.index)
        draw = self.randomness.get_draw(pop_data.index)
        priority_index = [i for d, i in sorted(zip(draw, pop_data.index), key=lambda x:x[0])]
        self.sample_index = pd.Index(priority_index[:sample_size])

    def record(self, event):
        pop = self.population_view.get(self.sample_index)

        pipeline_results = []
        for name, pipeline in self.pipelines.items():
            values = pipeline(self.sample_index)
            if name == 'mortality_rate':
                values = values.sum(axis=1)
            values = values.rename(name)
            pipeline_results.append(values)

            if name == 'bmi_exposure':  # pipeline.source(index)
                raw_values = pipeline.source(self.sample_index)
                raw_values = raw_values.rename(f'{name}_baseline')
                pipeline_results.append(raw_values)

        record = pd.concat(pipeline_results + [pop], axis=1)
        record['time'] = self.clock()
        record.index.rename("simulant", inplace=True)
        record.set_index('time', append=True, inplace=True)

        self.history_snapshots.append(record)

    def dump(self, event):
        sample_history = pd.concat(self.history_snapshots, axis=0)
        sample_history.to_hdf(self.sample_history_parameters.path, key='histories')
