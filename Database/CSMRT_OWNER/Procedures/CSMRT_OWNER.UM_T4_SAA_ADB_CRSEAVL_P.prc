DROP PROCEDURE CSMRT_OWNER.UM_T4_SAA_ADB_CRSEAVL_P
/

--
-- UM_T4_SAA_ADB_CRSEAVL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_T4_SAA_ADB_CRSEAVL_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table UM_T4_SAA_ADB_CRSEAVL from PeopleSoft table UM_T4_SAA_ADB_CRSEAVL.
--
 --V01  SMT-xxxx 06/20/2018,    James Doucette
--                              Converted from SQL Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_T4_SAA_ADB_CRSEAVL';
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
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_T4_SAA_ADB_CRSEAVL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_T4_SAA_ADB_CRSEAVL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_T4_SAA_ADB_CRSEAVL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_T4_SAA_ADB_CRSEAVL';				
insert /*+ append */ into CSMRT_OWNER.UM_T4_SAA_ADB_CRSEAVL
with RES as (
select /*+ parallel(8) inline */ 
       R.EMPLID, R.ANALYSIS_DB_SEQ, R.SAA_CAREER_RPT, R.SAA_ENTRY_SEQ, R.SRC_SYS_ID, 
       R.INSTITUTION_CD --R.INSTITUTION_SID, R.ACAD_CAR_SID, R.TERM_SID, R.PERSON_SID
  from CSMRT_OWNER.UM_F_SAA_ADB_RESULTS R
 where R.CRSES_REQUIRED < 999
   and R.ITEM_R_STATUS in ('FAIL')
   and R.TSCRPT_TYPE in ('DADV','LADV')
--   and not (R.INSTITUTION_CD = 'UMLOW' and R.SAA_CAREER_RPT = 'UGRD' and R.SAA_RPT_IDENTIFIER = 'LADVALL')
--   and not  R.SAA_RPT_IDENTIFIER = 'LADVALL' 
--   and not  R.SAA_RPT_IDENTIFIER in ('ADMIN','DADVBTCH') 
   and substr(EMPLID,-1,1) in ('8','9')
   ), 
OFF1 as (
select /*+ parallel(8) inline */
       O.CRSE_ID, 
       O.EFFDT, 
       O.CRSE_OFFER_NBR, 
       O.SRC_SYS_ID, 
       O.INSTITUTION INSTITUTION_CD,
       O.ACAD_CAREER ACAD_CAR_CD,
       O.SUBJECT,
       O.CATALOG_NBR, 
       O.RQRMNT_GROUP, 
       row_number() over (partition by O.CRSE_ID, O.CRSE_OFFER_NBR, O.SRC_SYS_ID
                              order by O.EFFDT desc) OFF_ORDER
  from CSSTG_OWNER.PS_CRSE_OFFER O
 where O.DATA_ORIGIN <> 'D'
   and ROWNUM < 1000000000),        -- Sept 2021 
OFF2 as ( 
select /*+ parallel(8) inline */
       O.CRSE_ID, 
       O.EFFDT, 
       O.CRSE_OFFER_NBR, 
       O.SRC_SYS_ID, 
       O.INSTITUTION_CD,
       O.ACAD_CAR_CD,
       O.SUBJECT,
       O.CATALOG_NBR, 
       O.RQRMNT_GROUP, 
       row_number() over (partition by O.CRSE_ID, O.INSTITUTION_CD, O.ACAD_CAR_CD, O.SUBJECT, O.CATALOG_NBR, O.SRC_SYS_ID
                              order by O.EFFDT desc, O.CRSE_OFFER_NBR) OFF_ORDER
  from OFF1 O
 where O.OFF_ORDER = 1),
AVL1 as (
select /*+ parallel(8) inline */ 
       RES.EMPLID, RES.ANALYSIS_DB_SEQ, RES.SAA_CAREER_RPT, RES.SAA_ENTRY_SEQ, CRSA.SAA_COURSE_SEQ, RES.SRC_SYS_ID,
       RES.INSTITUTION_CD, --RES.INSTITUTION_SID, RES.ACAD_CAR_SID, RES.TERM_SID, RES.PERSON_SID,
       CRSA.COURSE_LIST, CRSA.R_COURSE_SEQUENCE, CRSA.SUBJECT, CRSA.CATALOG_NBR,
       nvl(CRSA.CRSE_ID,'-') CRSE_ID, nvl(OFF2.CRSE_OFFER_NBR,0) CRSE_OFFER_NBR, 
       nvl(OFF2.RQRMNT_GROUP,'-') CRSE_RQRMNT_GROUP,
       CRSA.CRS_TOPIC_ID
  from RES
  join CSSTG_OWNER.PS_SAA_ADB_CRSEAVL CRSA
    on RES.EMPLID = CRSA.EMPLID
   and RES.ANALYSIS_DB_SEQ = CRSA.ANALYSIS_DB_SEQ
   and RES.SAA_CAREER_RPT = CRSA.SAA_CAREER_RPT
   and RES.SAA_ENTRY_SEQ = CRSA.SAA_ENTRY_SEQ
   and RES.SRC_SYS_ID = CRSA.SRC_SYS_ID
  left outer join OFF2 
    on CRSA.CRSE_ID = OFF2.CRSE_ID
   and RES.INSTITUTION_CD = OFF2.INSTITUTION_CD
   and RES.SAA_CAREER_RPT = OFF2.ACAD_CAR_CD
   and CRSA.SUBJECT = OFF2.SUBJECT
   and CRSA.CATALOG_NBR = OFF2.CATALOG_NBR
   and RES.SRC_SYS_ID = OFF2.SRC_SYS_ID
   and OFF2.OFF_ORDER = 1
 where nvl(trim(CRSA.CRSE_ID),'-') <> '-'),
AVL2 as (
select /*+ parallel(8) inline */ 
       EMPLID, ANALYSIS_DB_SEQ, SAA_CAREER_RPT, SAA_ENTRY_SEQ, SAA_COURSE_SEQ, SRC_SYS_ID,
       INSTITUTION_CD, --INSTITUTION_SID, ACAD_CAR_SID, TERM_SID, PERSON_SID,
       COURSE_LIST, R_COURSE_SEQUENCE, SUBJECT, CATALOG_NBR,
       CRSE_ID, CRSE_OFFER_NBR, 
       CRSE_RQRMNT_GROUP,
       CRS_TOPIC_ID,
       '-' PLAN_TERM_CD
  from AVL1
 where CRSE_OFFER_NBR > 0
 union all
select /*+ parallel(8) inline */ 
       AVL1.EMPLID, AVL1.ANALYSIS_DB_SEQ, AVL1.SAA_CAREER_RPT, AVL1.SAA_ENTRY_SEQ, AVL1.SAA_COURSE_SEQ, AVL1.SRC_SYS_ID,
       AVL1.INSTITUTION_CD, --AVL1.INSTITUTION_SID, AVL1.ACAD_CAR_SID, AVL1.TERM_SID, AVL1.PERSON_SID,
       AVL1.COURSE_LIST, AVL1.R_COURSE_SEQUENCE, AVL1.SUBJECT, AVL1.CATALOG_NBR,
       AVL1.CRSE_ID, 1 CRSE_OFFER_NBR, 
       nvl(OFF1.RQRMNT_GROUP,'-') CRSE_RQRMNT_GROUP,
       AVL1.CRS_TOPIC_ID,
       '-' PLAN_TERM_CD
  from AVL1
  left outer join OFF1 
    on AVL1.CRSE_ID = OFF1.CRSE_ID
   and 1 = OFF1.CRSE_OFFER_NBR
   and AVL1.SRC_SYS_ID = OFF1.SRC_SYS_ID
   and OFF1.OFF_ORDER = 1 
 where AVL1.CRSE_OFFER_NBR = 0
 union all
select /*+ parallel(8) inline */ 
       RES.EMPLID, RES.ANALYSIS_DB_SEQ, RES.SAA_CAREER_RPT, RES.SAA_ENTRY_SEQ, 1000+P.SAA_PLNR_CRSE_SEQ SAA_COURSE_SEQ, RES.SRC_SYS_ID,  
       RES.INSTITUTION_CD, --RES.INSTITUTION_SID, RES.ACAD_CAR_SID, RES.TERM_SID, RES.PERSON_SID,
       '-' COURSE_LIST, 0 R_COURSE_SEQUENCE, P.SUBJECT, P.CATALOG_NBR,
       P.CRSE_ID, P.CRSE_OFFER_NBR, 
       nvl(OFF1.RQRMNT_GROUP,'-') RQRMNT_GROUP,
       P.CRS_TOPIC_ID,
       nvl(trim(P.STRM),'-') PLAN_TERM_CD
  from RES
  join CSSTG_OWNER.PS_SSS_CRSE_PLNR P
    on RES.EMPLID = P.EMPLID
   and RES.SAA_CAREER_RPT = P.ACAD_CAREER
   and RES.INSTITUTION_CD = P.INSTITUTION
   and RES.SRC_SYS_ID = P.SRC_SYS_ID
  left outer join OFF1 
    on P.CRSE_ID = OFF1.CRSE_ID
--   and P.CRSE_OFFER_NBR = OFF1.CRSE_OFFER_NBR     -- Feb 2017 
   and 1 = OFF1.CRSE_OFFER_NBR
   and RES.SRC_SYS_ID = OFF1.SRC_SYS_ID
   and OFF1.OFF_ORDER = 1 
 where nvl(trim(P.STRM),'-') <> '-'),
GRP1 as (
select /*+ parallel(8) inline */ 
       RQRMNT_GROUP, EFFDT, SRC_SYS_ID,
       row_number() over (partition by RQRMNT_GROUP, SRC_SYS_ID order by EFFDT desc) RQ_GRP_ORDER
  from CSSTG_OWNER.PS_RQ_GRP_TBL
 where DATA_ORIGIN <> 'D'),
GRP2 as (
select /*+ parallel(8) inline */ 
       GRP1.RQRMNT_GROUP, DTL.SRC_SYS_ID, 
       sum(case when DTL.REQUISITE_TYPE = 'CO' then 1 else 0 end) CO_REQ_CNT,
       count(*) GRP_CNT
  from GRP1
  join CSSTG_OWNER.PS_RQ_GRP_DETL_TBL DTL
    on GRP1.RQRMNT_GROUP = DTL.RQRMNT_GROUP
   and GRP1.EFFDT = DTL.EFFDT
   and GRP1.SRC_SYS_ID = DTL.SRC_SYS_ID
   and DTL.DATA_ORIGIN <> 'D'
   and GRP1.RQ_GRP_ORDER = 1
 group by GRP1.RQRMNT_GROUP, DTL.SRC_SYS_ID),
COURSES as (
select /*+ parallel(8) inline */ distinct 
       EMPLID, 
       ANALYSIS_DB_SEQ, 
       SAA_CAREER_RPT, 
       SRC_SYS_ID, 
       CRSE_ID,
       EARN_CREDIT   
  from CSSTG_OWNER.PS_SAA_ADB_COURSES
 where nvl(trim(EARN_CREDIT),'-') = 'Y'
   and nvl(trim(VALID_ATTEMPT),'-') = 'Y'
--   and nvl(trim(CRSE_GRADE_OFF),'-') not in ('-','INC','W')     -- Should we exlcude INC? What about W???   
)
select /*+ parallel(8) inline */ 
AVL2.EMPLID,
AVL2.ANALYSIS_DB_SEQ,
AVL2.SAA_CAREER_RPT,
AVL2.SAA_ENTRY_SEQ,
AVL2.SAA_COURSE_SEQ,
AVL2.SRC_SYS_ID,
AVL2.INSTITUTION_CD,
--AVL2.INSTITUTION_SID,
--AVL2.ACAD_CAR_SID,
--AVL2.TERM_SID,
--AVL2.PERSON_SID,
nvl(C.CRSE_SID,2147483646) CRSE_SID,        -- Added Feb 2017 
nvl(T.TERM_SID,2147483646) PLAN_TERM_SID,   -- Added Feb 2017 
AVL2.COURSE_LIST,
AVL2.R_COURSE_SEQUENCE,
AVL2.SUBJECT,
AVL2.CATALOG_NBR,
AVL2.CRSE_ID,
AVL2.CRSE_OFFER_NBR,        -- Added Feb 2017 
AVL2.CRSE_RQRMNT_GROUP,     -- Added Dec 2016 
(case when nvl(AVL2.CRSE_RQRMNT_GROUP,'-') = '-' 
      then '-'
      when nvl(GRP2.CO_REQ_CNT,0) = 0
      then '-'
      when nvl(GRP2.CO_REQ_CNT,0) > 0 and nvl(GRP2.CO_REQ_CNT,0) = nvl(GRP2.GRP_CNT,0) 
      then 'Y' 
      else 'N' end) CO_REQ_ONLY_FLG,     -- Added Dec 2016 
AVL2.CRS_TOPIC_ID,
nvl(COURSES.EARN_CREDIT,'N') EARN_CREDIT,  
(case when nvl(trim(AVL2.CRSE_RQRMNT_GROUP),'-') = '-' 
      then '-' 
 else GET_PRE_REQ_MET_FLG(trim(replace(replace(replace(G.SQL_STR_PRE,'AAAAA',AVL2.EMPLID),22222,AVL2.ANALYSIS_DB_SEQ),'CCCCC',AVL2.SAA_CAREER_RPT))) end) PRE_REQ_MET_FLG, 
SYSDATE CREATED_EW_DTTM
  from AVL2
  left outer join CSMRT_OWNER.UM_D_CRSE C
    on AVL2.CRSE_ID = C.CRSE_CD 
   and AVL2.CRSE_OFFER_NBR = C.CRSE_OFFER_NUM 
   and AVL2.SRC_SYS_ID = C.SRC_SYS_ID
  left outer join CSMRT_OWNER.PS_D_TERM T
    on AVL2.INSTITUTION_CD = T.INSTITUTION_CD 
   and AVL2.SAA_CAREER_RPT = T.ACAD_CAR_CD
   and AVL2.PLAN_TERM_CD = T.TERM_CD
   and AVL2.SRC_SYS_ID = T.SRC_SYS_ID
  left outer join COURSES 
    on AVL2.EMPLID = COURSES.EMPLID
   and AVL2.ANALYSIS_DB_SEQ = COURSES.ANALYSIS_DB_SEQ
   and AVL2.SAA_CAREER_RPT = COURSES.SAA_CAREER_RPT
   and AVL2.SRC_SYS_ID = COURSES.SRC_SYS_ID
   and AVL2.CRSE_ID = COURSES.CRSE_ID
  left outer join CSMRT_OWNER.UM_D_RQ_GRP G   
    on AVL2.CRSE_RQRMNT_GROUP = G.RQRMNT_GROUP
   and AVL2.SRC_SYS_ID = G.SRC_SYS_ID
   and G.DATA_ORIGIN <> 'D'
  left outer join GRP2   
    on AVL2.CRSE_RQRMNT_GROUP = GRP2.RQRMNT_GROUP
   and AVL2.SRC_SYS_ID = GRP2.SRC_SYS_ID
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_T4_SAA_ADB_CRSEAVL rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_T4_SAA_ADB_CRSEAVL',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_T4_SAA_ADB_CRSEAVL',
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

END UM_T4_SAA_ADB_CRSEAVL_P;
/
