package com.viewstar.service;

public interface SeqRouteAnalysisDao {

    public int saveSeqRoute(String id, String jsoninfo);
    public int updateSeqRoute(String id, String jsoninfo);

    public String getID();
}
