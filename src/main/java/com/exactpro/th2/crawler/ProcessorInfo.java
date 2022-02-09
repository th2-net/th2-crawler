package com.exactpro.th2.crawler;

public class ProcessorInfo {
    private String dataProcessorName;
    private String dataProcessorVersion;

    public ProcessorInfo(String dataProcessorName, String dataProcessorVersion) {
        this.dataProcessorName = dataProcessorName;
        this.dataProcessorVersion = dataProcessorVersion;
    }

    public String getDataProcessorName() {
        return dataProcessorName;
    }

    public void setDataProcessorName(String dataProcessorName) {
        this.dataProcessorName = dataProcessorName;
    }

    public String getDataProcessorVersion() {
        return dataProcessorVersion;
    }

    public void setDataProcessorVersion(String dataProcessorVersion) {
        this.dataProcessorVersion = dataProcessorVersion;
    }
}
