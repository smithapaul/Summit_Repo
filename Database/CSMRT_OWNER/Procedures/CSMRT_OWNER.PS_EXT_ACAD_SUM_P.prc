CREATE OR REPLACE PROCEDURE             "PS_EXT_ACAD_SUM_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--
-- Loads stage table PS_EXT_ACAD_SUM from PeopleSoft table PS_EXT_ACAD_SUM.
--
-- V01    SMT-xxxx 09/06/2017,    James Doucette
--                                Converted from DataStage
-- V01.2  SMT-8300 09/06/2017,    James Doucette
--                                Added two new fields.
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_EXT_ACAD_SUM';
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

strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


strSqlCommand   := 'update START_DT on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Reading',
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_EXT_ACAD_SUM'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_EXT_ACAD_SUM@SASOURCE S)
 where TABLE_NAME = 'PS_EXT_ACAD_SUM'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_EXT_ACAD_SUM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_EXT_ACAD_SUM';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_EXT_ACAD_SUM T 
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(EXT_ORG_ID),'-') EXT_ORG_ID, 
    nvl(trim(EXT_CAREER),'-') EXT_CAREER, 
    nvl(EXT_DATA_NBR,0) EXT_DATA_NBR, 
    nvl(trim(EXT_SUMM_TYPE),'-') EXT_SUMM_TYPE, 
    nvl(trim(EXT_ACAD_LEVEL),'-') EXT_ACAD_LEVEL, 
    nvl(TERM_YEAR,0) TERM_YEAR, 
    nvl(trim(EXT_TERM_TYPE),'-') EXT_TERM_TYPE, 
    nvl(trim(EXT_TERM),'-') EXT_TERM, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(UNT_TYPE),'-') UNT_TYPE, 
    nvl(UNT_ATMP_TOTAL,0) UNT_ATMP_TOTAL, 
    nvl(UNT_COMP_TOTAL,0) UNT_COMP_TOTAL, 
    nvl(CLASS_RANK,0) CLASS_RANK, 
    nvl(CLASS_SIZE,0) CLASS_SIZE, 
    nvl(trim(GPA_TYPE),'-') GPA_TYPE, 
    nvl(EXT_GPA,0) EXT_GPA, 
    nvl(CONVERT_GPA,0) CONVERT_GPA, 
    nvl(PERCENTILE,0) PERCENTILE, 
    nvl(trim(RANK_TYPE),'-') RANK_TYPE, 
    nvl(trim(UM_GPA_EXCLUDE),'-') UM_GPA_EXCLUDE, 
    nvl(UM_EXT_ORG_CR,0) UM_EXT_ORG_CR, 
    nvl(UM_EXT_ORG_QP,0) UM_EXT_ORG_QP, 
    nvl(UM_EXT_ORG_GPA,0) UM_EXT_ORG_GPA, 
    nvl(UM_EXT_ORG_CNV_CR,0) UM_EXT_ORG_CNV_CR, 
    nvl(UM_EXT_ORG_CNV_GPA,0) UM_EXT_ORG_CNV_GPA, 
    nvl(UM_EXT_ORG_CNV_QP,0) UM_EXT_ORG_CNV_QP, 
    nvl(trim(UM_GPA_OVERRIDE),'-') UM_GPA_OVERRIDE, 
    nvl(trim(UM_1_OVR_HSGPA),'-') UM_1_OVR_HSGPA, 
    nvl(UM_CONVERT_GPA,0) UM_CONVERT_GPA,
	nvl(UM_EXT_OR_MTSC_GPA,0) UM_EXT_OR_MTSC_GPA,           -- SMT-8300
	nvl(MS_CONVERT_GPA,0) MS_CONVERT_GPA                    -- SMT-8300
from SYSADM.PS_EXT_ACAD_SUM@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_EXT_ACAD_SUM') 
  and EMPLID between '00000000' and '99999999'
  and length(EMPLID) = 8 ) S 
 on ( 
    T.EMPLID = S.EMPLID and 
    T.EXT_ORG_ID = S.EXT_ORG_ID and 
    T.EXT_CAREER = S.EXT_CAREER and 
    T.EXT_DATA_NBR = S.EXT_DATA_NBR and 
    T.EXT_SUMM_TYPE = S.EXT_SUMM_TYPE and 
    T.INSTITUTION = S.INSTITUTION and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.EXT_ACAD_LEVEL = S.EXT_ACAD_LEVEL,
    T.TERM_YEAR = S.TERM_YEAR,
    T.EXT_TERM_TYPE = S.EXT_TERM_TYPE,
    T.EXT_TERM = S.EXT_TERM,
    T.UNT_TYPE = S.UNT_TYPE,
    T.UNT_ATMP_TOTAL = S.UNT_ATMP_TOTAL,
    T.UNT_COMP_TOTAL = S.UNT_COMP_TOTAL,
    T.CLASS_RANK = S.CLASS_RANK,
    T.CLASS_SIZE = S.CLASS_SIZE,
    T.GPA_TYPE = S.GPA_TYPE,
    T.EXT_GPA = S.EXT_GPA,
    T.CONVERT_GPA = S.CONVERT_GPA,
    T.PERCENTILE = S.PERCENTILE,
    T.RANK_TYPE = S.RANK_TYPE,
    T.UM_GPA_EXCLUDE = S.UM_GPA_EXCLUDE,
    T.UM_EXT_ORG_CR = S.UM_EXT_ORG_CR,
    T.UM_EXT_ORG_QP = S.UM_EXT_ORG_QP,
    T.UM_EXT_ORG_GPA = S.UM_EXT_ORG_GPA,
    T.UM_EXT_ORG_CNV_CR = S.UM_EXT_ORG_CNV_CR,
    T.UM_EXT_ORG_CNV_GPA = S.UM_EXT_ORG_CNV_GPA,
    T.UM_EXT_ORG_CNV_QP = S.UM_EXT_ORG_CNV_QP,
    T.UM_GPA_OVERRIDE = S.UM_GPA_OVERRIDE,
    T.UM_1_OVR_HSGPA = S.UM_1_OVR_HSGPA,
    T.UM_CONVERT_GPA = S.UM_CONVERT_GPA,
	T.UM_EXT_OR_MTSC_GPA = S.UM_EXT_OR_MTSC_GPA,            -- SMT-8300
	T.MS_CONVERT_GPA = S.MS_CONVERT_GPA,                    -- SMT-8300
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.EXT_ACAD_LEVEL <> S.EXT_ACAD_LEVEL or 
    T.TERM_YEAR <> S.TERM_YEAR or 
    T.EXT_TERM_TYPE <> S.EXT_TERM_TYPE or 
    T.EXT_TERM <> S.EXT_TERM or 
    T.UNT_TYPE <> S.UNT_TYPE or 
    T.UNT_ATMP_TOTAL <> S.UNT_ATMP_TOTAL or 
    T.UNT_COMP_TOTAL <> S.UNT_COMP_TOTAL or 
    T.CLASS_RANK <> S.CLASS_RANK or 
    T.CLASS_SIZE <> S.CLASS_SIZE or 
    T.GPA_TYPE <> S.GPA_TYPE or 
    T.EXT_GPA <> S.EXT_GPA or 
    T.CONVERT_GPA <> S.CONVERT_GPA or 
    T.PERCENTILE <> S.PERCENTILE or 
    T.RANK_TYPE <> S.RANK_TYPE or 
    T.UM_GPA_EXCLUDE <> S.UM_GPA_EXCLUDE or 
    T.UM_EXT_ORG_CR <> S.UM_EXT_ORG_CR or 
    T.UM_EXT_ORG_QP <> S.UM_EXT_ORG_QP or 
    T.UM_EXT_ORG_GPA <> S.UM_EXT_ORG_GPA or 
    T.UM_EXT_ORG_CNV_CR <> S.UM_EXT_ORG_CNV_CR or 
    T.UM_EXT_ORG_CNV_GPA <> S.UM_EXT_ORG_CNV_GPA or 
    T.UM_EXT_ORG_CNV_QP <> S.UM_EXT_ORG_CNV_QP or 
    T.UM_GPA_OVERRIDE <> S.UM_GPA_OVERRIDE or 
    T.UM_1_OVR_HSGPA <> S.UM_1_OVR_HSGPA or 
    T.UM_CONVERT_GPA <> S.UM_CONVERT_GPA or 
	T.UM_EXT_OR_MTSC_GPA <> S.UM_EXT_OR_MTSC_GPA or      -- SMT-8300
	T.MS_CONVERT_GPA <> S.MS_CONVERT_GPA or              -- SMT-8300
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.EXT_ORG_ID, 
    T.EXT_CAREER, 
    T.EXT_DATA_NBR, 
    T.EXT_SUMM_TYPE,
    T.SRC_SYS_ID, 
    T.EXT_ACAD_LEVEL, 
    T.TERM_YEAR,
    T.EXT_TERM_TYPE,
    T.EXT_TERM, 
    T.INSTITUTION,
    T.UNT_TYPE, 
    T.UNT_ATMP_TOTAL, 
    T.UNT_COMP_TOTAL, 
    T.CLASS_RANK, 
    T.CLASS_SIZE, 
    T.GPA_TYPE, 
    T.EXT_GPA,
    T.CONVERT_GPA,
    T.PERCENTILE, 
    T.RANK_TYPE,
    T.UM_GPA_EXCLUDE, 
    T.UM_EXT_ORG_CR,
    T.UM_EXT_ORG_QP,
    T.UM_EXT_ORG_GPA, 
    T.UM_EXT_ORG_CNV_CR,
    T.UM_EXT_ORG_CNV_GPA, 
    T.UM_EXT_ORG_CNV_QP,
    T.UM_GPA_OVERRIDE,
    T.UM_1_OVR_HSGPA, 
    T.UM_CONVERT_GPA, 
	T.UM_EXT_OR_MTSC_GPA,                -- SMT-8300
	T.MS_CONVERT_GPA,                    -- SMT-8300
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
    values (
    S.EMPLID, 
    S.EXT_ORG_ID, 
    S.EXT_CAREER, 
    S.EXT_DATA_NBR, 
    S.EXT_SUMM_TYPE,
    'CS90', 
    S.EXT_ACAD_LEVEL, 
    S.TERM_YEAR,
    S.EXT_TERM_TYPE,
    S.EXT_TERM, 
    S.INSTITUTION,
    S.UNT_TYPE, 
    S.UNT_ATMP_TOTAL, 
    S.UNT_COMP_TOTAL, 
    S.CLASS_RANK, 
    S.CLASS_SIZE, 
    S.GPA_TYPE, 
    S.EXT_GPA,
    S.CONVERT_GPA,
    S.PERCENTILE, 
    S.RANK_TYPE,
    S.UM_GPA_EXCLUDE, 
    S.UM_EXT_ORG_CR,
    S.UM_EXT_ORG_QP,
    S.UM_EXT_ORG_GPA, 
    S.UM_EXT_ORG_CNV_CR,
    S.UM_EXT_ORG_CNV_GPA, 
    S.UM_EXT_ORG_CNV_QP,
    S.UM_GPA_OVERRIDE,
    S.UM_1_OVR_HSGPA, 
    S.UM_CONVERT_GPA, 
	S.UM_EXT_OR_MTSC_GPA,                -- SMT-8300
	S.MS_CONVERT_GPA,                    -- SMT-8300
    'N',
    'S',
    sysdate,
    sysdate,
    1234)
;



strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_EXT_ACAD_SUM rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_EXT_ACAD_SUM',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_EXT_ACAD_SUM';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_EXT_ACAD_SUM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_EXT_ACAD_SUM';
update CSSTG_OWNER.PS_EXT_ACAD_SUM T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, EXT_ORG_ID, EXT_CAREER, EXT_DATA_NBR, EXT_SUMM_TYPE, INSTITUTION
   from CSSTG_OWNER.PS_EXT_ACAD_SUM T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_EXT_ACAD_SUM') = 'Y'
  minus
 select EMPLID, EXT_ORG_ID, EXT_CAREER, EXT_DATA_NBR, EXT_SUMM_TYPE, INSTITUTION
   from SYSADM.PS_EXT_ACAD_SUM@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_EXT_ACAD_SUM') = 'Y' 
   ) S
 where T.EMPLID = S.EMPLID   
   AND T.EXT_ORG_ID = S.EXT_ORG_ID
   AND T.EXT_CAREER = S.EXT_CAREER
   AND T.EXT_DATA_NBR = S.EXT_DATA_NBR
   AND T.EXT_SUMM_TYPE = S.EXT_SUMM_TYPE
   AND T.INSTITUTION = S.INSTITUTION
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_EXT_ACAD_SUM rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_EXT_ACAD_SUM',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_EXT_ACAD_SUM'
;

strSqlCommand := 'commit';
commit;


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

END PS_EXT_ACAD_SUM_P;
/
