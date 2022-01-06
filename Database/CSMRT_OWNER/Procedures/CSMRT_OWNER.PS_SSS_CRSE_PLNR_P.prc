CREATE OR REPLACE PROCEDURE             "PS_SSS_CRSE_PLNR_P" AUTHID CURRENT_USER IS

/*
-- Run before the first time
DELETE
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_SSS_CRSE_PLNR'

INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
(TABLE_NAME, DELETE_FLG)
VALUES
('PS_SSS_CRSE_PLNR', 'Y')

SELECT *
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_SSS_CRSE_PLNR'
*/


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_SSS_CRSE_PLNR from PeopleSoft table PS_SSS_CRSE_PLNR.
--
-- V01  SMT-xxxx 05/15/2017,    Jim Doucette
--                              Converted from PS_SSS_CRSE_PLNR.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_SSS_CRSE_PLNR';
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
 where TABLE_NAME = 'PS_SSS_CRSE_PLNR'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_SSS_CRSE_PLNR@SASOURCE S)
 where TABLE_NAME = 'PS_SSS_CRSE_PLNR'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_SSS_CRSE_PLNR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_SSS_CRSE_PLNR';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_SSS_CRSE_PLNR T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(SAA_PLNR_SEQ,0) SAA_PLNR_SEQ, 
    nvl(SAA_PLNR_CRSE_SEQ,0) SAA_PLNR_CRSE_SEQ, 
    nvl(trim(CRSE_ID),'-') CRSE_ID, 
    nvl(CRSE_OFFER_NBR,0) CRSE_OFFER_NBR, 
    nvl(CRS_TOPIC_ID,0) CRS_TOPIC_ID, 
    nvl(trim(SUBJECT),'-') SUBJECT, 
    nvl(trim(CATALOG_NBR),'-') CATALOG_NBR, 
    nvl(UNT_TAKEN,0) UNT_TAKEN, 
    nvl(trim(STRM),'-') STRM, 
    SAA_ADD_RG SAA_ADD_RG,
    SAA_ADD_RQ SAA_ADD_RQ,
    SAA_ADD_RQL SAA_ADD_RQL,
    SAA_USED_RG SAA_USED_RG,
    SAA_USED_RQ SAA_USED_RQ,
    SAA_USED_RQL SAA_USED_RQL,
    LASTUPDOPRID LASTUPDOPRID,
    to_date(to_char(case when LASTUPDDTTM < '01-JAN-1800' then NULL 
                    else LASTUPDDTTM end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LASTUPDDTTM
  from SYSADM.PS_SSS_CRSE_PLNR@SASOURCE S 
 where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SSS_CRSE_PLNR')
   and EMPLID BETWEEN '00000000' AND '99999999'
   and length(EMPLID) = 8 ) S
 on ( T.EMPLID = S.EMPLID and 
    T.ACAD_CAREER = S.ACAD_CAREER and 
    T.INSTITUTION = S.INSTITUTION and 
    T.SAA_PLNR_SEQ = S.SAA_PLNR_SEQ and 
    T.SAA_PLNR_CRSE_SEQ = S.SAA_PLNR_CRSE_SEQ and 
    T.SRC_SYS_ID = 'CS90')
 when matched then update set
    T.CRSE_ID = S.CRSE_ID,
    T.CRSE_OFFER_NBR = S.CRSE_OFFER_NBR,
    T.CRS_TOPIC_ID = S.CRS_TOPIC_ID,
    T.SUBJECT = S.SUBJECT,
    T.CATALOG_NBR = S.CATALOG_NBR,
    T.UNT_TAKEN = S.UNT_TAKEN,
    T.STRM = S.STRM,
    T.SAA_ADD_RG = S.SAA_ADD_RG,
    T.SAA_ADD_RQ = S.SAA_ADD_RQ,
    T.SAA_ADD_RQL = S.SAA_ADD_RQL,
    T.SAA_USED_RG = S.SAA_USED_RG,
    T.SAA_USED_RQ = S.SAA_USED_RQ,
    T.SAA_USED_RQL = S.SAA_USED_RQL,
    T.LASTUPDOPRID = S.LASTUPDOPRID,
    T.LASTUPDDTTM = S.LASTUPDDTTM,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.CRSE_ID <> S.CRSE_ID or 
    T.CRSE_OFFER_NBR <> S.CRSE_OFFER_NBR or 
    T.CRS_TOPIC_ID <> S.CRS_TOPIC_ID or 
    T.SUBJECT <> S.SUBJECT or 
    T.CATALOG_NBR <> S.CATALOG_NBR or 
    T.UNT_TAKEN <> S.UNT_TAKEN or 
    T.STRM <> S.STRM or 
    nvl(trim(T.SAA_ADD_RG),0) <> nvl(trim(S.SAA_ADD_RG),0) or 
    nvl(trim(T.SAA_ADD_RQ),0) <> nvl(trim(S.SAA_ADD_RQ),0) or 
    nvl(trim(T.SAA_ADD_RQL),0) <> nvl(trim(S.SAA_ADD_RQL),0) or 
    nvl(trim(T.SAA_USED_RG),0) <> nvl(trim(S.SAA_USED_RG),0) or 
    nvl(trim(T.SAA_USED_RQ),0) <> nvl(trim(S.SAA_USED_RQ),0) or 
    nvl(trim(T.SAA_USED_RQL),0) <> nvl(trim(S.SAA_USED_RQL),0) or 
    nvl(trim(T.LASTUPDOPRID),0) <> nvl(trim(S.LASTUPDOPRID),0) or 
    nvl(trim(T.LASTUPDDTTM),0) <> nvl(trim(S.LASTUPDDTTM),0) or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.ACAD_CAREER,
    T.INSTITUTION,
    T.SAA_PLNR_SEQ, 
    T.SAA_PLNR_CRSE_SEQ,
    T.SRC_SYS_ID, 
    T.CRSE_ID,
    T.CRSE_OFFER_NBR, 
    T.CRS_TOPIC_ID, 
    T.SUBJECT,
    T.CATALOG_NBR,
    T.UNT_TAKEN,
    T.STRM, 
    T.SAA_ADD_RG, 
    T.SAA_ADD_RQ, 
    T.SAA_ADD_RQL,
    T.SAA_USED_RG,
    T.SAA_USED_RQ,
    T.SAA_USED_RQL, 
    T.LASTUPDOPRID, 
    T.LASTUPDDTTM,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.EMPLID, 
    S.ACAD_CAREER,
    S.INSTITUTION,
    S.SAA_PLNR_SEQ, 
    S.SAA_PLNR_CRSE_SEQ,
    'CS90', 
    S.CRSE_ID,
    S.CRSE_OFFER_NBR, 
    S.CRS_TOPIC_ID, 
    S.SUBJECT,
    S.CATALOG_NBR,
    S.UNT_TAKEN,
    S.STRM, 
    S.SAA_ADD_RG, 
    S.SAA_ADD_RQ, 
    S.SAA_ADD_RQL,
    S.SAA_USED_RG,
    S.SAA_USED_RQ,
    S.SAA_USED_RQL, 
    S.LASTUPDOPRID, 
    S.LASTUPDDTTM,
    'N',
    'S',
    sysdate,
    sysdate,
    1234);


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_SSS_CRSE_PLNR rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_SSS_CRSE_PLNR',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_SSS_CRSE_PLNR';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_SSS_CRSE_PLNR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_SSS_CRSE_PLNR';
update CSSTG_OWNER.PS_SSS_CRSE_PLNR T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, ACAD_CAREER, INSTITUTION, SAA_PLNR_SEQ, SAA_PLNR_CRSE_SEQ
   from CSSTG_OWNER.PS_SSS_CRSE_PLNR T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SSS_CRSE_PLNR') = 'Y'
  minus
 select EMPLID, ACAD_CAREER, INSTITUTION, SAA_PLNR_SEQ, SAA_PLNR_CRSE_SEQ
   from SYSADM.PS_SSS_CRSE_PLNR@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SSS_CRSE_PLNR') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.INSTITUTION = S.INSTITUTION
   and T.SAA_PLNR_SEQ = S.SAA_PLNR_SEQ
   and T.SAA_PLNR_CRSE_SEQ = S.SAA_PLNR_CRSE_SEQ
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_SSS_CRSE_PLNR rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_SSS_CRSE_PLNR',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_SSS_CRSE_PLNR'
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

END PS_SSS_CRSE_PLNR_P;
/
