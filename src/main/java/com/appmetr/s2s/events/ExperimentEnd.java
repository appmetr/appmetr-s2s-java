package com.appmetr.s2s.events;

public class ExperimentEnd extends Experiment {
    public ExperimentEnd(String experiment) {
        super(Experiment.STATUS_END, experiment);
    }
}
