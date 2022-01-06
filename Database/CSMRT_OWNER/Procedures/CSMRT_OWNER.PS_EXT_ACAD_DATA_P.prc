CREATE OR REPLACE PROCEDURE             "PS_EXT_ACAD_DATA_P" AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_EXT_ACAD_DATA from PeopleSoft table PS_EXT_ACAD_DATA.
--
 --V01  SMT-xxxx 09/05/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_EXT_ACAD_DATA';
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
 where TABLE_NAME = 'PS_EXT_ACAD_DATA'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_EXT_ACAD_DATA@SASOURCE S)
 where TABLE_NAME = 'PS_EXT_ACAD_DATA'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_EXT_ACAD_DATA';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_EXT_ACAD_DATA';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_EXT_ACAD_DATA T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(EXT_ORG_ID),'-') EXT_ORG_ID, 
    nvl(trim(EXT_CAREER),'-') EXT_CAREER, 
    nvl(EXT_DATA_NBR,0) EXT_DATA_NBR, 
    nvl(trim(LS_DATA_SOURCE),'-') LS_DATA_SOURCE, 
    nvl(trim(TRANSCRIPT_FLAG),'-') TRANSCRIPT_FLAG, 
    nvl(trim(TRANSCRIPT_TYPE),'-') TRANSCRIPT_TYPE, 
    nvl(trim(TRNSCRPT_STATUS),'-') TRNSCRPT_STATUS, 
    nvl(trim(EXT_ACAD_LEVEL),'-') EXT_ACAD_LEVEL, 
    NVL(TRANSCRIPT_DT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )) TRANSCRIPT_DT, 
    nvl(trim(EXT_TERM_TYPE),'-') EXT_TERM_TYPE, 
    nvl(trim(EXT_TERM),'-') EXT_TERM, 
    nvl(TERM_YEAR,0) TERM_YEAR, 
    '-' RECEIVED_FLAG, 
    NVL(RECEIVED_DT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )) RECEIVED_DT, 
    nvl(trim(DATA_MEDIUM),'-') DATA_MEDIUM, 
    NVL(FROM_DT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )) FROM_DT, 
    NVL(TO_DT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )) TO_DT
from SYSADM.PS_EXT_ACAD_DATA@SASOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_EXT_ACAD_DATA') 
  and EMPLID between '00000000' and '99999999'
  and length(EMPLID) = 8) S
 on ( 
    T.EMPLID = S.EMPLID and 
    T.EXT_ORG_ID = S.EXT_ORG_ID and 
    T.EXT_CAREER = S.EXT_CAREER and 
    T.EXT_DATA_NBR = S.EXT_DATA_NBR and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.LS_DATA_SOURCE = S.LS_DATA_SOURCE,
    T.TRANSCRIPT_FLAG = S.TRANSCRIPT_FLAG,
    T.TRANSCRIPT_TYPE = S.TRANSCRIPT_TYPE,
    T.TRNSCRPT_STATUS = S.TRNSCRPT_STATUS,
    T.EXT_ACAD_LEVEL = S.EXT_ACAD_LEVEL,
    T.TRANSCRIPT_DT = S.TRANSCRIPT_DT,
    T.EXT_TERM_TYPE = S.EXT_TERM_TYPE,
    T.EXT_TERM = S.EXT_TERM,
    T.TERM_YEAR = S.TERM_YEAR,
    T.RECEIVED_FLAG = S.RECEIVED_FLAG,
    T.RECEIVED_DT = S.RECEIVED_DT,
    T.DATA_MEDIUM = S.DATA_MEDIUM,
    T.FROM_DT = S.FROM_DT,
    T.TO_DT = S.TO_DT,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.LS_DATA_SOURCE <> S.LS_DATA_SOURCE or 
    T.TRANSCRIPT_FLAG <> S.TRANSCRIPT_FLAG or 
    T.TRANSCRIPT_TYPE <> S.TRANSCRIPT_TYPE or 
    T.TRNSCRPT_STATUS <> S.TRNSCRPT_STATUS or 
    T.EXT_ACAD_LEVEL <> S.EXT_ACAD_LEVEL or 
    nvl(trim(T.TRANSCRIPT_DT),0) <> nvl(trim(S.TRANSCRIPT_DT),0) or 
    T.EXT_TERM_TYPE <> S.EXT_TERM_TYPE or 
    T.EXT_TERM <> S.EXT_TERM or 
    T.TERM_YEAR <> S.TERM_YEAR or 
    T.RECEIVED_FLAG <> S.RECEIVED_FLAG or 
    nvl(trim(T.RECEIVED_DT),0) <> nvl(trim(S.RECEIVED_DT),0) or 
    T.DATA_MEDIUM <> S.DATA_MEDIUM or 
    nvl(trim(T.FROM_DT),0) <> nvl(trim(S.FROM_DT),0) or 
    nvl(trim(T.TO_DT),0) <> nvl(trim(S.TO_DT),0) or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.EXT_ORG_ID, 
    T.EXT_CAREER, 
    T.EXT_DATA_NBR, 
    T.SRC_SYS_ID, 
    T.LS_DATA_SOURCE, 
    T.TRANSCRIPT_FLAG,
    T.TRANSCRIPT_TYPE,
    T.TRNSCRPT_STATUS,
    T.EXT_ACAD_LEVEL, 
    T.TRANSCRIPT_DT,
    T.EXT_TERM_TYPE,
    T.EXT_TERM, 
    T.TERM_YEAR,
    T.RECEIVED_FLAG,
    T.RECEIVED_DT,
    T.DATA_MEDIUM,
    T.FROM_DT,
    T.TO_DT,
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
    'CS90', 
    S.LS_DATA_SOURCE, 
    S.TRANSCRIPT_FLAG,
    S.TRANSCRIPT_TYPE,
    S.TRNSCRPT_STATUS,
    S.EXT_ACAD_LEVEL, 
    S.TRANSCRIPT_DT,
    S.EXT_TERM_TYPE,
    S.EXT_TERM, 
    S.TERM_YEAR,
    S.RECEIVED_FLAG,
    S.RECEIVED_DT,
    S.DATA_MEDIUM,
    S.FROM_DT,
    S.TO_DT,
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

strMessage01    := '# of PS_EXT_ACAD_DATA rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_EXT_ACAD_DATA',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_EXT_ACAD_DATA';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_EXT_ACAD_DATA';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_EXT_ACAD_DATA';
update CSSTG_OWNER.PS_EXT_ACAD_DATA T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, EXT_ORG_ID, EXT_CAREER, EXT_DATA_NBR
   from CSSTG_OWNER.PS_EXT_ACAD_DATA T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_EXT_ACAD_DATA') = 'Y'
  minus
 select EMPLID, EXT_ORG_ID, EXT_CAREER, EXT_DATA_NBR
   from SYSADM.PS_EXT_ACAD_DATA@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_EXT_ACAD_DATA') = 'Y' 
   ) S
 where T.EMPLID = S.EMPLID   
   and T.EXT_ORG_ID = S.EXT_ORG_ID
   and T.EXT_CAREER = S.EXT_CAREER
   and T.EXT_DATA_NBR = S.EXT_DATA_NBR
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_EXT_ACAD_DATA rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_EXT_ACAD_DATA',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_EXT_ACAD_DATA'
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

END PS_EXT_ACAD_DATA_P;
/
