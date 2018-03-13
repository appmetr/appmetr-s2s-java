package com.appmetr.s2s.events;

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

    @Override public String toString() {
        return "Experiment{" +
                "status='" + status + '\'' +
                ", experiment='" + experiment + '\'' +
                "} " + super.toString();
    }
}
