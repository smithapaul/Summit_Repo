CREATE OR REPLACE PROCEDURE             "PS_F_DEGREES_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
-- Loads table             -- PS_F_DEGREES
--PS_F_DEGREES         -- PS_D_INSTITUTION ;PS_D_ACAD_CAR;PS_D_PERSON;PS_D_DEG;UM_D_ACAD_PLAN;UM_D_ACAD_SPLAN
                           -- PS_D_DEG_STAT;PS_D_TERM;PS_D_DEG_HONORS
-- V01 11/5/2018          -- srikanth ,pabbu converted to proc from sql 

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_F_DEGREES';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.PS_F_DEGREES';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','PS_F_DEGREES');

strSqlDynamic   := 'alter table CSMRT_OWNER.PS_F_DEGREES disable constraint PK_PS_F_DEGREES';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.PS_F_DEGREES';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.PS_F_DEGREES';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.PS_F_DEGREES';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.PS_F_DEGREES';				
insert /*+ append */ into PS_F_DEGREES 
  select /*+ parallel(8) */
       DG.EMPLID PERSON_ID, 
       to_number(DG.STDNT_DEGR) DEGREE_NBR,
       DP.ACAD_PLAN ACAD_PLAN_CD, 
       nvl(DS.ACAD_SUB_PLAN,'-') ACAD_SPLAN_CD, 
       DG.SRC_SYS_ID,
       DG.INSTITUTION INSTITUTION_CD,
       DG.ACAD_CAREER ACAD_CAR_CD,
       DG.DEGREE DEG_CD, 
       nvl(I.INSTITUTION_SID, 2147483646) INSTITUTION_SID, 
	   nvl(C.ACAD_CAR_SID, 2147483646) ACAD_CAR_SID, 
       nvl(P.PERSON_SID, 2147483646) PERSON_SID,
       nvl(D.DEG_SID, 2147483646) DEG_SID,   
       nvl(AP.ACAD_PLAN_SID,2147483646) ACAD_PLAN_SID, 
       nvl(AS1.ACAD_SPLAN_SID,2147483646) ACAD_SPLAN_SID, 	   
       nvl(DS1.DEG_STAT_SID,2147483646) ACAD_DEGR_STAT_SID, 
	   nvl(C1.ACAD_CAR_SID,2147483646) ACAD_PLAN_CAR_SID,
	   nvl(T.TERM_SID,2147483646) COMPL_TERM_SID, 
	   nvl(H.DEG_HONORS_SID,2147483646) HONORS_PREFIX_SID,
       nvl(H1.DEG_HONORS_SID,2147483646) HONORS_SUFFIX_SID, 
       '2147483646' PLN_HONRS_PREF_SID, 
	   '2147483646' PLN_HONRS_SUFF_SID, 
       '2147483646' SPLN_HNRS_PREF_SID, 
       '2147483646' SPLN_HNRS_SUFF_SID, 
       DG.CLASS_RANK_NBR,               -- Always zero??? 
       DG.CLASS_RANK_TOT,               -- Always zero??? 
       DG.DEGR_CONFER_DT CONF_DT, 
       DG.DEGR_STATUS_DATE DEGR_STAT_DT,
       DG.GPA_DEGREE,
       DP.GPA_PLAN,                                 -- Always zero???
       DP.CLASS_RANK_NBR PLAN_CLASS_RANK_NBR,       -- Always zero???
       DP.CLASS_RANK_TOT PLAN_CLASS_RANK_TOT,       -- Always zero???
       DP.ACAD_DEGR_STATUS PLAN_DEGR_STATUS, 
       DP.DEGR_STATUS_DATE PLN_DEG_ST_DT,
	   case when  nvl(DP.DIPLOMA_DESCR,'-') = '-' then nvl(AP.DIPLOMA_DESCR,'-') else DP.DIPLOMA_DESCR end PLAN_DIPLOMA_DESCR,
       DP.OVERRIDE_FL PLAN_OVERRIDE_FLG,
       DP.PLAN_SEQUENCE,
       DP.TRNSCR_DESCR PLAN_TRNSCR_DESCR,
	   case when nvl(DS.DIPLOMA_DESCR,'-') = '-' then nvl(AS1.DIPLOMA_LD,'-') else DS.DIPLOMA_DESCR end SPLAN_DIPLOMA_DESCR,
       nvl(DS.OVERRIDE_FL,'-') SPLAN_OVERRIDE_FLG,
       nvl(DS.SUB_PLAN_SEQUENCE,0) SPLAN_SEQUENCE,
       nvl(DS.TRNSCR_DESCR,'-') SPLAN_TRNSCR_DESCR,
       DP.STDNT_CAR_NBR,
       case when DG.ACAD_DEGR_STATUS = 'A' then 1 else 0 end DEGREE_COUNT_AWD,  
       case when DG.ACAD_DEGR_STATUS = 'R' then 1 else 0 end DEGREE_COUNT_RVK,  
       1 DEGREE_COUNT,
       'N' LOAD_ERROR, 
       'S' DATA_ORIGIN, 
       SYSDATE CREATED_EW_DTTM, 
       SYSDATE LASTUPD_EW_DTTM, 
       1234 BATCH_SID
  from CSSTG_OWNER.PS_ACAD_DEGR DG
  join CSSTG_OWNER.PS_ACAD_DEGR_PLAN DP 
    on DG.EMPLID = DP.EMPLID 
   and DG.STDNT_DEGR = DP.STDNT_DEGR 
   and DG.SRC_SYS_ID = DP.SRC_SYS_ID
   and DP.DATA_ORIGIN <> 'D'
  left outer join CSSTG_OWNER.PS_ACAD_DEGR_SPLN DS 
    on DP.EMPLID = DS.EMPLID 
   and DP.STDNT_DEGR = DS.STDNT_DEGR 
   and DP.ACAD_PLAN = DS.ACAD_PLAN 
   and DP.SRC_SYS_ID = DS.SRC_SYS_ID
   and DS.DATA_ORIGIN <> 'D'
   left outer join CSMRT_OWNER.PS_D_INSTITUTION I
   on DG.INSTITUTION = I.INSTITUTION_CD
   and DG.SRC_SYS_ID = I.SRC_SYS_ID
   and I.DATA_ORIGIN <> 'D'
   left outer join CSMRT_OWNER.PS_D_ACAD_CAR C
    on DG.ACAD_CAREER = C.ACAD_CAR_CD 
   and DG.INSTITUTION = C.INSTITUTION_CD	
   and DG.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
   left outer join CSMRT_OWNER.PS_D_PERSON P
    on DG.EMPLID = P.PERSON_ID  
   and DG.SRC_SYS_ID = P.SRC_SYS_ID
   and P.DATA_ORIGIN <> 'D' 
   left outer join CSMRT_OWNER.PS_D_DEG D
    on DG.DEGREE = D.DEG_CD  
   and DG.SRC_SYS_ID = D.SRC_SYS_ID
   and D.DATA_ORIGIN <> 'D' 
   left outer join CSMRT_OWNER.UM_D_ACAD_PLAN AP
    on DG.INSTITUTION = AP.INSTITUTION_CD
   and DP.ACAD_PLAN = AP.ACAD_PLAN_CD  
   and DG.SRC_SYS_ID = AP.SRC_SYS_ID
   and AP.DATA_ORIGIN <> 'D' 
   and AP.EFFDT_ORDER=1
   left outer join CSMRT_OWNER.UM_D_ACAD_SPLAN AS1
    on DG.INSTITUTION = AS1.INSTITUTION_CD
    and DP.ACAD_PLAN = AS1.ACAD_PLAN_CD
    and DS.ACAD_SUB_PLAN =AS1.ACAD_SPLAN_CD	
   and DG.SRC_SYS_ID = AS1.SRC_SYS_ID
   and AS1.DATA_ORIGIN <> 'D' 
   and AS1.EFFDT_ORDER=1
    left outer join CSMRT_OWNER.PS_D_DEG_STAT DS1
    on DG.ACAD_DEGR_STATUS = DS1.DEG_STAT_CD  
   and DG.SRC_SYS_ID = DS1.SRC_SYS_ID
   and DS1.DATA_ORIGIN <> 'D' 
  left outer join CSMRT_OWNER.PS_D_ACAD_CAR C1
    on DG.ACAD_CAREER = C1.ACAD_CAR_CD 
   and DG.INSTITUTION = C1.INSTITUTION_CD	
   and DG.SRC_SYS_ID = C1.SRC_SYS_ID
   and C1.DATA_ORIGIN <> 'D'
     left outer join CSMRT_OWNER.PS_D_TERM T	
    on DG.INSTITUTION = T.INSTITUTION_CD
   and DG.ACAD_CAREER = T.ACAD_CAR_CD
   and DG.COMPLETION_TERM = T.TERM_CD
   and DG.SRC_SYS_ID = T.SRC_SYS_ID 
   and T.DATA_ORIGIN <> 'D' 
   left outer join CSMRT_OWNER.PS_D_DEG_HONORS H
    on DG.INSTITUTION=H.INSTITUTION_CD
   and H.HONORS_TYPE_CD='DP' 
   and H.HONORS_CD=DG.HONORS_PREFIX
   and H.DATA_ORIGIN <> 'D'
   left outer join CSMRT_OWNER.PS_D_DEG_HONORS H1
    on DG.INSTITUTION=H1.INSTITUTION_CD
   and H1.HONORS_TYPE_CD='DH' 
   and H1.HONORS_CD=DG.HONORS_SUFFIX
   and H1.DATA_ORIGIN <> 'D' 
 where DG.DATA_ORIGIN <> 'D'
   and substr(DG.STDNT_DEGR,1,1) between '0' and '9'
   and substr(DG.STDNT_DEGR,2,1) between '0' and '9'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_F_DEGREES rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_F_DEGREES',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_F_DEGREES',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.PS_F_DEGREES';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.PS_F_DEGREES enable constraint PK_PS_F_DEGREES';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','PS_F_DEGREES');

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

END PS_F_DEGREES_P;
/
