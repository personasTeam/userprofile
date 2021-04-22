package com.viewstar.controller;

import com.viewstar.service.impl.SeqRouteAnalysisServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;


@RestController
@ResponseBody
public class SeqRouteAnalysisController {

    @Autowired
    private SeqRouteAnalysisServer seqRouteAnalysisServer;

    @RequestMapping(value = {"/sparkUseranalysis/saveSeqRouteData"}, params = {"sqllist"}, method = {RequestMethod.GET, RequestMethod.POST})
    public String saveSeqRouteData(@RequestParam(value = "sqllist")String sqllist){
        String result = "";
        try {
            System.out.println(sqllist);
            result = seqRouteAnalysisServer.saveSeqRouteData(sqllist);
        } catch (Exception e) {
            e.printStackTrace();
            result = e.getMessage();
        }
        return result;
    }


    @RequestMapping(value = {"/sparkUseranalysis/updateSeqRouteData"}, params = {"sqllist","behaviorseq"}, method = {RequestMethod.GET, RequestMethod.POST})
    public String updateSeqRouteData(@RequestParam(value = "parID")String parID,
                                     @RequestParam(value = "sqllist")String sqllist,
                                   @RequestParam(value = "behaviorseq")String behaviorseq){
        String result = "";
        try {
            result = seqRouteAnalysisServer.updateSeqRouteData(parID,sqllist,behaviorseq);
        } catch (Exception e) {
            e.printStackTrace();
            result = e.getMessage();
        }
        return result;
    }

}
