package com.appmetr.s2s.events;

import java.util.Objects;

public abstract class Experiment extends Action{
    private static final String ACTION = "trackExperiment";

    protected static final String STATUS_ON = "ON";
    protected static final String STATUS_END = "END";

    private String status;
    private String experiment;

    //Only for Jackson deserialization
    private Experiment() {
        super(ACTION);
    }

    public Experiment(String status, String experiment) {
        this();
        this.status = status;
        this.experiment = experiment;
    }

    public String getStatus() {
        return status;
    }

    public String getExperiment() {
        return experiment;
    }

    @Override public int calcApproximateSize() {
        return super.calcApproximateSize() + getStringLength(status) + getStringLength(experiment);
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        Experiment that = (Experiment) o;
        return Objects.equals(getStatus(), that.getStatus()) &&
                Objects.equals(getExperiment(), that.getExperiment());
    }

    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), getStatus(), getExperiment());
    }

    @Override public String toString() {
        return "Experiment{" +
                "status='" + getStatus() + '\'' +
                ", experiment='" + getExperiment() + '\'' +
                "} " + super.toString();
    }
}
