package com.appmetr.s2s.events;

public class ExperimentStart extends Experiment {
    private String group;

    public ExperimentStart(String experiment, String group) {
        super(Experiment.STATUS_ON, experiment);

        this.group = group;
    }

    public String getGroup() {
        return group;
    }

    @Override public int calcApproximateSize() {
        return super.calcApproximateSize() + getStringLength(group);
    }
}
