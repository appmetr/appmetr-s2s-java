package com.appmetr.s2s.events;

public class ExperimentEnd extends Experiment {

    //Only for Jackson deserialization
    private ExperimentEnd() {
        super(Experiment.STATUS_END, null);
    }

    public ExperimentEnd(String experiment) {
        super(Experiment.STATUS_END, experiment);
    }

    @Override public String toString() {
        return "ExperimentEnd{} " + super.toString();
    }
}
