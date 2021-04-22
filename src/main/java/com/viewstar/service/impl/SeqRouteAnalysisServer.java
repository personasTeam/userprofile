package com.viewstar.service.impl;

import com.viewstar.service.SeqRouteAnalysisDao;
import com.viewstar.thread.SeqRouteAnalysisThread;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

@Controller
public class SeqRouteAnalysisServer {

    @Autowired
    private SeqRouteAnalysisThread asyncThread;

    @Autowired
    private SeqRouteAnalysisDao seqRouteAnalysisDao;


    public String saveSeqRouteData(String sqllist){
//        String parID = seqRouteAnalysisDao.getID();
        String s = asyncThread.asyncSaveSeqRouteData(sqllist);
//        seqRouteAnalysisDao.saveSeqRoute(parID,sqllist);
        return s;
    }

    public String updateSeqRouteData(String parID,String sqllist,String behaviorseq){
        asyncThread.asyncSaveSeqRouteData(sqllist);
        seqRouteAnalysisDao.updateSeqRoute(parID,sqllist);
        return parID;
    }
}
