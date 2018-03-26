package com.appmetr.s2s.events;

import java.util.Objects;

public class ExperimentStart extends Experiment {
    private String group;

    //Only for Jackson deserialization
    public ExperimentStart() {
        super(Experiment.STATUS_ON, null);
    }

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

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        ExperimentStart that = (ExperimentStart) o;
        return Objects.equals(getGroup(), that.getGroup());
    }

    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), getGroup());
    }

    @Override public String toString() {
        return "ExperimentStart{" +
                "group='" + getGroup() + '\'' +
                "} " + super.toString();
    }
}
