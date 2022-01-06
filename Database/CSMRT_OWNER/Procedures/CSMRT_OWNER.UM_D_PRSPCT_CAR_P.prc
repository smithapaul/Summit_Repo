CREATE OR REPLACE PROCEDURE             "UM_D_PRSPCT_CAR_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- James Doucette
--
-- Loads stage table UM_D_PRSPCT_CAR from stage table table PS_ADM_PRSPCT_CAR.
--
-- V01  SMT-xxxx 2/12/2019,    James Doucette
--                             Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_D_PRSPCT_CAR';
        intProcessSid                   Integer;
        dtProcessStart                  Date            := SYSDATE;
        strMessage01                    Varchar2(4000);
        strMessage02                    Varchar2(512);
        strMessage03                    Varchar2(512)   :='';
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strSqlCommand                   Varchar2(32767) :='';
        strSqlDynamic                   Varchar2(32767) :='';
        strClientInfo                   Varchar2(100);
        intRowCount                     Integer;
        intTotalRowCount                Integer         := 0;
        numSqlCode                      Number;
        strSqlErrm                      Varchar2(4000);
        intTries                        Integer;

BEGIN

strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_INIT';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
        (
                i_MartId                => strMartId,
                i_ProcessName           => strProcessName,
                i_ProcessStartTime      => dtProcessStart,
                o_ProcessSid            => intProcessSid
        );

strMessage01    := 'Merging data into CSMRT_OWNER.UM_D_PRSPCT_CAR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSMRT_OWNER.UM_D_PRSPCT_CAR';
merge /*+ use_hash(S,T) */ into CSMRT_OWNER.UM_D_PRSPCT_CAR T 
using (                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
  with Q1 as (  
SELECT EMPLID, ACAD_CAREER, INSTITUTION, SRC_SYS_ID,
       ADMIT_TERM, ADMIT_TYPE, ADM_RECR_CTR, LAST_SCH_ATTEND, GRADUATION_DT, 
       RECRUITING_STATUS, RECR_STATUS_DT, APPL_ON_FILE, 
       FIN_AID_INTEREST, HOUSING_INTEREST, ACAD_LOAD_APPR, ADM_REFRL_SRCE, REFERRAL_SRCE_DT, 
       --REGION, REGION_FROM,   -- Drop???  
       RECRUITER_ID, ADM_CREATION_DT, ACADEMIC_LEVEL, CAMPUS, DATA_ORIGIN
  FROM CSSTG_OWNER.PS_ADM_PRSPCT_CAR    -- NK --> EMPLID, ACAD_CAREER, INSTITUTION, SRC_SYS_ID
), 
       S as (
select Q1.INSTITUTION INSTITUTION_CD, Q1.ACAD_CAREER ACAD_CAR_CD, Q1.ADMIT_TERM, Q1.EMPLID, Q1.SRC_SYS_ID, 
       nvl(AC.ACAD_CAR_SID,2147483646) ACAD_CAR_SID, 
       nvl(AL.ACAD_LOAD_SID,2147483646) ACAD_LOAD_SID, 
       nvl(AV.ACAD_LVL_SID,2147483646) ACAD_LVL_SID, 
       Q1.ADM_CREATION_DT, 
       Q1.ADM_RECR_CTR, 
       nvl(T.TERM_SID,2147483646) ADMIT_TERM_SID, 
       nvl(AT.ADMIT_TYPE_SID,2147483646) ADMIT_TYPE_SID, 
       Q1.APPL_ON_FILE, 
       nvl(C.CAMPUS_SID,2147483646) CAMPUS_SID, 
       Q1.FIN_AID_INTEREST, 
       Q1.HOUSING_INTEREST, 
       nvl(I.INSTITUTION_SID,2147483646) INSTITUTION_SID, 
       nvl(EO.EXT_ORG_SID,2147483646) LST_SCHL_ATTND_SID, 
       to_number(to_char(Q1.GRADUATION_DT,'YYYYMMDD')) LST_SCHL_GRDDT_SID, 
       nvl(P.PERSON_SID,2147483646) PERSON_SID, 
       nvl(RC.RECRT_CNTR_SID,2147483646) RECRT_CNTR_SID, 
       nvl(RS.RECRT_STAT_SID,2147483646) RECRT_STAT_SID, 
       to_number(to_char(Q1.RECR_STATUS_DT,'YYYYMMDD')) RECRT_STAT_DT_SID, 
       nvl(RR.RECRTR_SID,2147483646) RECRTR_SID, 
       2147483646 REGION_CS_SID, 
       '-' REGION_FROM, 
       nvl(RF.RFRL_SRC_SID,2147483646) RFRL_SRC_SID, 
       to_number(to_char(Q1.REFERRAL_SRCE_DT,'YYYYMMDD')) RFRL_SRC_DT_SID, 
       Q1.DATA_ORIGIN 
  from Q1
  left outer join PS_D_ACAD_CAR AC  
    on Q1.INSTITUTION = AC.INSTITUTION_CD
   and Q1.ACAD_CAREER = AC.ACAD_CAR_CD
   and Q1.SRC_SYS_ID = AC.SRC_SYS_ID
   and AC.DATA_ORIGIN <> 'D'
  left outer join PS_D_ACAD_LOAD AL  
    on AL.APPRVD_IND in ('-', 'Y')
   and Q1.ACAD_LOAD_APPR = AL.ACAD_LOAD_CD
   and Q1.SRC_SYS_ID = AL.SRC_SYS_ID
   and AL.DATA_ORIGIN <> 'D'
  left outer join PS_D_ACAD_LVL AV  
    on Q1.ACADEMIC_LEVEL = AV.ACAD_LVL_CD
   and Q1.SRC_SYS_ID = AV.SRC_SYS_ID
   and AV.DATA_ORIGIN <> 'D'
  left outer join PS_D_TERM T 
    on Q1.INSTITUTION = T.INSTITUTION_CD
   and Q1.ACAD_CAREER = T.ACAD_CAR_CD
   and Q1.ADMIT_TERM = T.TERM_CD
   and Q1.SRC_SYS_ID = T.SRC_SYS_ID
   and T.DATA_ORIGIN <> 'D'
  left outer join PS_D_ADMIT_TYPE AT  
    on Q1.INSTITUTION = AT.INSTITUTION_CD
   and Q1.ADMIT_TYPE = AT.ADMIT_TYPE_ID
   and Q1.SRC_SYS_ID = AT.SRC_SYS_ID
   and AT.DATA_ORIGIN <> 'D'
  left outer join PS_D_CAMPUS C  
    on Q1.INSTITUTION = C.INSTITUTION_CD
   and Q1.CAMPUS = C.CAMPUS_CD
   and Q1.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
  left outer join PS_D_INSTITUTION I  
    on Q1.INSTITUTION = I.INSTITUTION_CD
   and Q1.SRC_SYS_ID = I.SRC_SYS_ID
   and I.DATA_ORIGIN <> 'D'
  left outer join PS_D_EXT_ORG EO  
    on Q1.LAST_SCH_ATTEND = EO.EXT_ORG_ID
   and Q1.SRC_SYS_ID = EO.SRC_SYS_ID
   and EO.DATA_ORIGIN <> 'D'
  left outer join PS_D_PERSON P  
    on Q1.EMPLID = P.PERSON_ID
   and Q1.SRC_SYS_ID = P.SRC_SYS_ID
   and P.DATA_ORIGIN <> 'D'
  left outer join PS_D_RECRT_CNTR RC 
    on Q1.INSTITUTION = RC.INSTITUTION_CD   -- Change to INSTITUTION_CD when _NEW is replaced!!!
   and Q1.ADM_RECR_CTR = RC.RECRT_CNTR_ID
   and Q1.SRC_SYS_ID = RC.SRC_SYS_ID
   and RC.DATA_ORIGIN <> 'D'
  left outer join PS_D_RECRT_STAT RS 
    on Q1.RECRUITING_STATUS = RS.RECRT_STAT_ID
   and Q1.SRC_SYS_ID = RS.SRC_SYS_ID
   and RS.DATA_ORIGIN <> 'D'
  left outer join PS_D_RECRTR RR 
    on Q1.INSTITUTION = RR.INSTITUTION_CD 
   and Q1.ACAD_CAREER = RR.ACAD_CAR_CD
   and Q1.RECRUITER_ID = RR.RECRUITER_ID
   and Q1.SRC_SYS_ID = RR.SRC_SYS_ID
   and RR.DATA_ORIGIN <> 'D'
  left outer join PS_D_RFRL_SRC RF  
    on Q1.ADM_REFRL_SRCE = RF.RFRL_SRC_ID
   and Q1.SRC_SYS_ID = RF.SRC_SYS_ID
   and RF.DATA_ORIGIN <> 'D'
    )
--select nvl(D.PRSPCT_CAR_SID, max(D.PRSPCT_CAR_SID) over (partition by 1) +                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
--       row_number() over (partition by 1 order by D.PRSPCT_CAR_SID nulls first)) PRSPCT_CAR_SID,
select nvl(D.PRSPCT_CAR_SID, 
          (select nvl(max(PRSPCT_CAR_SID),0) from CSMRT_OWNER.UM_D_PRSPCT_CAR where PRSPCT_CAR_SID < 2147483646) + 
                  row_number() over (partition by 1 order by D.PRSPCT_CAR_SID nulls first)) PRSPCT_CAR_SID,         -- Mar 2020 
       nvl(D.INSTITUTION_CD, S.INSTITUTION_CD) INSTITUTION_CD, 
       nvl(D.ACAD_CAR_CD, S.ACAD_CAR_CD) ACAD_CAR_CD, 
       nvl(D.ADMIT_TERM, S.ADMIT_TERM) ADMIT_TERM, 
       nvl(D.EMPLID, S.EMPLID) EMPLID, 
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID,
       decode(D.ACAD_CAR_SID, S.ACAD_CAR_SID, D.ACAD_CAR_SID, S.ACAD_CAR_SID) ACAD_CAR_SID,
       decode(D.ACAD_LOAD_SID, S.ACAD_LOAD_SID, D.ACAD_LOAD_SID, S.ACAD_LOAD_SID) ACAD_LOAD_SID,
       decode(D.ACAD_LVL_SID, S.ACAD_LVL_SID, D.ACAD_LVL_SID, S.ACAD_LVL_SID) ACAD_LVL_SID,
       decode(D.ADM_CREATION_DT, S.ADM_CREATION_DT, D.ADM_CREATION_DT, S.ADM_CREATION_DT) ADM_CREATION_DT,
       decode(D.ADM_RECR_CTR, S.ADM_RECR_CTR, D.ADM_RECR_CTR, S.ADM_RECR_CTR) ADM_RECR_CTR,
       decode(D.ADMIT_TERM_SID, S.ADMIT_TERM_SID, D.ADMIT_TERM_SID, S.ADMIT_TERM_SID) ADMIT_TERM_SID,
       decode(D.ADMIT_TYPE_SID, S.ADMIT_TYPE_SID, D.ADMIT_TYPE_SID, S.ADMIT_TYPE_SID) ADMIT_TYPE_SID,
       decode(D.APPL_ON_FILE, S.APPL_ON_FILE, D.APPL_ON_FILE, S.APPL_ON_FILE) APPL_ON_FILE,
       decode(D.CAMPUS_SID, S.CAMPUS_SID, D.CAMPUS_SID, S.CAMPUS_SID) CAMPUS_SID,
       decode(D.FIN_AID_INTEREST, S.FIN_AID_INTEREST, D.FIN_AID_INTEREST, S.FIN_AID_INTEREST) FIN_AID_INTEREST,
       decode(D.HOUSING_INTEREST, S.HOUSING_INTEREST, D.HOUSING_INTEREST, S.HOUSING_INTEREST) HOUSING_INTEREST,
       decode(D.INSTITUTION_SID, S.INSTITUTION_SID, D.INSTITUTION_SID, S.INSTITUTION_SID) INSTITUTION_SID,
       decode(D.LST_SCHL_ATTND_SID, S.LST_SCHL_ATTND_SID, D.LST_SCHL_ATTND_SID, S.LST_SCHL_ATTND_SID) LST_SCHL_ATTND_SID,
       decode(D.LST_SCHL_GRDDT_SID, S.LST_SCHL_GRDDT_SID, D.LST_SCHL_GRDDT_SID, S.LST_SCHL_GRDDT_SID) LST_SCHL_GRDDT_SID,
       decode(D.PERSON_SID, S.PERSON_SID, D.PERSON_SID, S.PERSON_SID) PERSON_SID,
       decode(D.RECRT_CNTR_SID, S.RECRT_CNTR_SID, D.RECRT_CNTR_SID, S.RECRT_CNTR_SID) RECRT_CNTR_SID,
       decode(D.RECRT_STAT_SID, S.RECRT_STAT_SID, D.RECRT_STAT_SID, S.RECRT_STAT_SID) RECRT_STAT_SID,
       decode(D.RECRT_STAT_DT_SID, S.RECRT_STAT_DT_SID, D.RECRT_STAT_DT_SID, S.RECRT_STAT_DT_SID) RECRT_STAT_DT_SID,
       decode(D.RECRTR_SID, S.RECRTR_SID, D.RECRTR_SID, S.RECRTR_SID) RECRTR_SID,
       decode(D.REGION_CS_SID, S.REGION_CS_SID, D.REGION_CS_SID, S.REGION_CS_SID) REGION_CS_SID,
       decode(D.REGION_FROM, S.REGION_FROM, D.REGION_FROM, S.REGION_FROM) REGION_FROM,
       decode(D.RFRL_SRC_SID, S.RFRL_SRC_SID, D.RFRL_SRC_SID, S.RFRL_SRC_SID) RFRL_SRC_SID,
       decode(D.RFRL_SRC_DT_SID, S.RFRL_SRC_DT_SID, D.RFRL_SRC_DT_SID, S.RFRL_SRC_DT_SID) RFRL_SRC_DT_SID,
       decode(D.DATA_ORIGIN, S.DATA_ORIGIN, D.DATA_ORIGIN, S.DATA_ORIGIN) DATA_ORIGIN,
       nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
       nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM 
  from S
left outer join CSMRT_OWNER.UM_D_PRSPCT_CAR D
   on D.PRSPCT_CAR_SID <> 2147483646
  and D.INSTITUTION_CD = S.INSTITUTION_CD
  and D.ACAD_CAR_CD = S.ACAD_CAR_CD 
  and D.ADMIT_TERM = S.ADMIT_TERM
  and D.EMPLID = S.EMPLID
  and D.SRC_SYS_ID = S.SRC_SYS_ID                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
) S                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
    on  (T.INSTITUTION_CD = S.INSTITUTION_CD                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
   and  T.ACAD_CAR_CD = S.ACAD_CAR_CD                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
   and  T.ADMIT_TERM = S.ADMIT_TERM                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
   and  T.EMPLID = S.EMPLID
   and  T.SRC_SYS_ID = S.SRC_SYS_ID)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
 when matched then update set                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
       T.ACAD_CAR_SID = S.ACAD_CAR_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       T.ACAD_LOAD_SID = S.ACAD_LOAD_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       T.ACAD_LVL_SID = S.ACAD_LVL_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
       T.ADM_CREATION_DT = S.ADM_CREATION_DT,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
       T.ADM_RECR_CTR = S.ADM_RECR_CTR, 
       T.ADMIT_TERM_SID = S.ADMIT_TERM_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       T.ADMIT_TYPE_SID = S.ADMIT_TYPE_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       T.APPL_ON_FILE = S.APPL_ON_FILE,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
       T.CAMPUS_SID = S.CAMPUS_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
       T.FIN_AID_INTEREST = S.FIN_AID_INTEREST, 
       T.HOUSING_INTEREST = S.HOUSING_INTEREST,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       T.INSTITUTION_SID = S.INSTITUTION_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       T.LST_SCHL_ATTND_SID = S.LST_SCHL_ATTND_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
       T.LST_SCHL_GRDDT_SID = S.LST_SCHL_GRDDT_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
       T.PERSON_SID = S.PERSON_SID, 
       T.RECRT_CNTR_SID = S.RECRT_CNTR_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       T.RECRT_STAT_SID = S.RECRT_STAT_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       T.RECRT_STAT_DT_SID = S.RECRT_STAT_DT_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
       T.RECRTR_SID = S.RECRTR_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
       T.REGION_CS_SID = S.REGION_CS_SID, 
       T.REGION_FROM = S.REGION_FROM,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       T.RFRL_SRC_SID = S.RFRL_SRC_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       T.RFRL_SRC_DT_SID = S.RFRL_SRC_DT_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
       T.DATA_ORIGIN = S.DATA_ORIGIN,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       T.LASTUPD_EW_DTTM = SYSDATE                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
 where                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
       decode(T.ACAD_CAR_SID,S.ACAD_CAR_SID,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
       decode(T.ACAD_LOAD_SID,S.ACAD_LOAD_SID,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
       decode(T.ACAD_LVL_SID,S.ACAD_LVL_SID,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
       decode(T.ADM_CREATION_DT,S.ADM_CREATION_DT,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       decode(T.ADM_RECR_CTR,S.ADM_RECR_CTR,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       decode(T.ADMIT_TERM_SID,S.ADMIT_TERM_SID,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       decode(T.ADMIT_TYPE_SID,S.ADMIT_TYPE_SID,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
       decode(T.APPL_ON_FILE,S.APPL_ON_FILE,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
       decode(T.CAMPUS_SID,S.CAMPUS_SID,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
       decode(T.FIN_AID_INTEREST,S.FIN_AID_INTEREST,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
	   decode(T.HOUSING_INTEREST,S.HOUSING_INTEREST,0,1) = 1 or
	   decode(T.INSTITUTION_SID,S.INSTITUTION_SID,0,1) = 1 or
	   decode(T.LST_SCHL_ATTND_SID,S.LST_SCHL_ATTND_SID,0,1) = 1 or
	   decode(T.LST_SCHL_GRDDT_SID,S.LST_SCHL_GRDDT_SID,0,1) = 1 or
	   decode(T.PERSON_SID,S.PERSON_SID,0,1) = 1 or
	   decode(T.RECRT_CNTR_SID,S.RECRT_CNTR_SID,0,1) = 1 or
	   decode(T.RECRT_STAT_SID,S.RECRT_STAT_SID,0,1) = 1 or
	   decode(T.RECRT_STAT_DT_SID,S.RECRT_STAT_DT_SID,0,1) = 1 or
	   decode(T.RECRTR_SID,S.RECRTR_SID,0,1) = 1 or
	   decode(T.REGION_CS_SID,S.REGION_CS_SID,0,1) = 1 or
	   decode(T.REGION_FROM,S.REGION_FROM,0,1) = 1 or
	   decode(T.RFRL_SRC_SID,S.RFRL_SRC_SID,0,1) = 1 or
	   decode(T.RFRL_SRC_DT_SID,S.RFRL_SRC_DT_SID,0,1) = 1 or
	   decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1	   
  when not matched then                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
insert (                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
       T.PRSPCT_CAR_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
       T.INSTITUTION_CD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
       T.ACAD_CAR_CD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
       T.ADMIT_TERM,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
       T.EMPLID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
       T.SRC_SYS_ID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       T.ACAD_CAR_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
       T.ACAD_LOAD_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       T.ACAD_LVL_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       T.ADM_CREATION_DT,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
       T.ADM_RECR_CTR,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
       T.ADMIT_TERM_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
       T.ADMIT_TYPE_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
       T.APPL_ON_FILE,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
       T.CAMPUS_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       T.FIN_AID_INTEREST,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
       T.HOUSING_INTEREST,
       T.INSTITUTION_SID, 
       T.LST_SCHL_ATTND_SID, 
       T.LST_SCHL_GRDDT_SID, 
       T.PERSON_SID, 
       T.RECRT_CNTR_SID, 
       T.RECRT_STAT_SID, 
       T.RECRT_STAT_DT_SID, 
       T.RECRTR_SID, 
       T.REGION_CS_SID, 
       T.REGION_FROM, 
       T.RFRL_SRC_SID, 
       T.RFRL_SRC_DT_SID,  
	   T.LOAD_ERROR,
       T.DATA_ORIGIN, 
       T.CREATED_EW_DTTM, 
       T.LASTUPD_EW_DTTM, 
       T.BATCH_SID	   
	   )                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
values (                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
       S.PRSPCT_CAR_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
       S.INSTITUTION_CD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
       S.ACAD_CAR_CD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
       S.ADMIT_TERM,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
       S.EMPLID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
       S.SRC_SYS_ID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       S.ACAD_CAR_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
       S.ACAD_LOAD_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       S.ACAD_LVL_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       S.ADM_CREATION_DT,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
       S.ADM_RECR_CTR,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
       S.ADMIT_TERM_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
       S.ADMIT_TYPE_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
       S.APPL_ON_FILE,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
       S.CAMPUS_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       S.FIN_AID_INTEREST,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
       S.HOUSING_INTEREST,
       S.INSTITUTION_SID, 
       S.LST_SCHL_ATTND_SID, 
       S.LST_SCHL_GRDDT_SID, 
       S.PERSON_SID, 
       S.RECRT_CNTR_SID, 
       S.RECRT_STAT_SID, 
       S.RECRT_STAT_DT_SID, 
       S.RECRTR_SID, 
       S.REGION_CS_SID, 
       S.REGION_FROM, 
       S.RFRL_SRC_SID, 
       S.RFRL_SRC_DT_SID, 
       'N',	   
       S.DATA_ORIGIN,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
       SYSDATE,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       SYSDATE,
	   '1234')
;                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_PRSPCT_CAR rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_PRSPCT_CAR',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating DATA_ORIGIN on CSMRT_OWNER.UM_D_PRSPCT_CAR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSMRT_OWNER.UM_D_PRSPCT_CAR';
update CSMRT_OWNER.UM_D_PRSPCT_CAR T
   set DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
   and T.PRSPCT_CAR_SID <> 2147483646
   and not exists (select 1
                     from CSSTG_OWNER.PS_ADM_PRSPCT_CAR S  
                    where T.INSTITUTION_CD = S.INSTITUTION
                      and T.ACAD_CAR_CD = S.ACAD_CAREER
                      and T.ADMIT_TERM = S.ADMIT_TERM
					  and T.EMPLID = S.EMPLID
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
                      and S.DATA_ORIGIN <> 'D')
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_PRSPCT_PROG rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_PRSPCT_CAR',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

EXCEPTION
    WHEN OTHERS THEN
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION
                (
                        i_SqlCommand   => strSqlCommand,
                        i_SqlCode      => SQLCODE,
                        i_SqlErrm      => SQLERRM
                );

END UM_D_PRSPCT_CAR_P;
/
