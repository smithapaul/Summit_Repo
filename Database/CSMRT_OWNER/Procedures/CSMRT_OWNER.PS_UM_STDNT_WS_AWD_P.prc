CREATE OR REPLACE PROCEDURE             "PS_UM_STDNT_WS_AWD_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_UM_STDNT_WS_AWD from PeopleSoft table PS_UM_STDNT_WS_AWD.
--
-- V01  SMT-xxxx 04/04/2017,    Jim Doucette
--                              Converted from PS_UM_STDNT_WS_AWD.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_UM_STDNT_WS_AWD';
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
 where TABLE_NAME = 'PS_UM_STDNT_WS_AWD'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_UM_STDNT_WS_AWD@SASOURCE S)
 where TABLE_NAME = 'PS_UM_STDNT_WS_AWD'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_UM_STDNT_WS_AWD';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_UM_STDNT_WS_AWD';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_UM_STDNT_WS_AWD T
using (select /*+ full(S) */
nvl(trim(EMPLID),'-') EMPLID,
nvl(trim(INSTITUTION),'-') INSTITUTION,
nvl(trim(AID_YEAR),'-') AID_YEAR,
nvl(trim(ITEM_TYPE),'-') ITEM_TYPE,
nvl(trim(ACAD_CAREER),'-') ACAD_CAREER,
nvl(SEQNO,0) SEQNO,
to_date(to_char(EFFDT,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT,
nvl(trim(EFF_STATUS),'-') EFF_STATUS,
nvl(EMPL_RCD,0) EMPL_RCD,
nvl(trim(AWARD_STATUS),'-') AWARD_STATUS,
to_date(to_char(ACTION_DT,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') ACTION_DT,
nvl(trim(WS_PLACEMENT_STAT),'-') WS_PLACEMENT_STAT,
to_date(to_char(WS_PLACEMENT_DT,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') WS_PLACEMENT_DT,
nvl(trim(COMMUNITY_SERVICE),'-') COMMUNITY_SERVICE,
to_date(to_char(JOB_REC_EFFDT,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') JOB_REC_EFFDT,
nvl(JOB_REC_EFFSEQ,0) JOB_REC_EFFSEQ,
nvl(trim(EMPLOYER),'-') EMPLOYER,
nvl(trim(PHONE),'-') PHONE,
nvl(trim(DEPTID),'-') DEPTID,
nvl(trim(LOCATION),'-') LOCATION,
nvl(trim(EMAILID),'-') EMAILID,
nvl(HOURLY_RT,0) HOURLY_RT,
nvl(trim(ACCOUNT),'-') ACCOUNT,
nvl(trim(PROFILE_ID),'-') PROFILE_ID,
to_date(to_char(END_DT,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') END_DT,
nvl(trim(COMMENTS_MSGS),'-') COMMENTS_MSGS,
nvl(trim(COUNTRY),'-') COUNTRY,
nvl(trim(ADDRESS1),'-') ADDRESS1,
nvl(trim(ADDRESS2),'-') ADDRESS2,
nvl(trim(ADDRESS3),'-') ADDRESS3,
nvl(trim(ADDRESS4),'-') ADDRESS4,
nvl(trim(CITY),'-') CITY,
nvl(trim(NUM1),'-') NUM1,
nvl(trim(NUM2),'-') NUM2,
nvl(trim(HOUSE_TYPE),'-') HOUSE_TYPE,
nvl(trim(ADDR_FIELD1),'-') ADDR_FIELD1,
nvl(trim(ADDR_FIELD2),'-') ADDR_FIELD2,
nvl(trim(ADDR_FIELD3),'-') ADDR_FIELD3,
nvl(trim(COUNTY),'-') COUNTY,
nvl(trim(STATE),'-') STATE,
nvl(trim(POSTAL),'-') POSTAL,
nvl(trim(GEO_CODE),'-') GEO_CODE,
nvl(trim(IN_CITY_LIMIT),'-') IN_CITY_LIMIT,
nvl(trim(UM_SEC_ACCOUNT),'-') UM_SEC_ACCOUNT,
nvl(trim(JOBID),'-') JOBID,
nvl(trim(SUPERVISOR_NAME),'-') SUPERVISOR_NAME,
to_date(to_char(START_DATE,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') START_DATE,
to_date(to_char(END_DATE,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') END_DATE,
nvl(trim(UM_EXEMPT),'-') UM_EXEMPT,
nvl(trim(UM_THIRD_ACCOUNT),'-') UM_THIRD_ACCOUNT,
nvl(trim(UM_FOURTH_ACCOUNT),'-') UM_FOURTH_ACCOUNT
  from SYSADM.PS_UM_STDNT_WS_AWD@SASOURCE S
 where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_STDNT_WS_AWD')
   and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8 ) S
   on (
T.EMPLID = S.EMPLID and
T.INSTITUTION = S.INSTITUTION and
T.AID_YEAR = S.AID_YEAR and
T.ITEM_TYPE = S.ITEM_TYPE and
T.ACAD_CAREER = S.ACAD_CAREER and
T.EFFDT = S.EFFDT and
T.SEQNO = S.SEQNO and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.EFF_STATUS = S.EFF_STATUS,
T.EMPL_RCD = S.EMPL_RCD,
T.AWARD_STATUS = S.AWARD_STATUS,
T.ACTION_DT = S.ACTION_DT,
T.WS_PLACEMENT_STAT = S.WS_PLACEMENT_STAT,
T.WS_PLACEMENT_DT = S.WS_PLACEMENT_DT,
T.COMMUNITY_SERVICE = S.COMMUNITY_SERVICE,
T.JOB_REC_EFFDT = S.JOB_REC_EFFDT,
T.JOB_REC_EFFSEQ = S.JOB_REC_EFFSEQ,
T.EMPLOYER = S.EMPLOYER,
T.PHONE = S.PHONE,
T.DEPTID = S.DEPTID,
T.LOCATION = S.LOCATION,
T.EMAILID = S.EMAILID,
T.HOURLY_RT = S.HOURLY_RT,
T.ACCOUNT = S.ACCOUNT,
T.PROFILE_ID = S.PROFILE_ID,
T.END_DT = S.END_DT,
T.COMMENTS_MSGS = S.COMMENTS_MSGS,
T.COUNTRY = S.COUNTRY,
T.ADDRESS1 = S.ADDRESS1,
T.ADDRESS2 = S.ADDRESS2,
T.ADDRESS3 = S.ADDRESS3,
T.ADDRESS4 = S.ADDRESS4,
T.CITY = S.CITY,
T.NUM1 = S.NUM1,
T.NUM2 = S.NUM2,
T.HOUSE_TYPE = S.HOUSE_TYPE,
T.ADDR_FIELD1 = S.ADDR_FIELD1,
T.ADDR_FIELD2 = S.ADDR_FIELD2,
T.ADDR_FIELD3 = S.ADDR_FIELD3,
T.COUNTY = S.COUNTY,
T.STATE = S.STATE,
T.POSTAL = S.POSTAL,
T.GEO_CODE = S.GEO_CODE,
T.IN_CITY_LIMIT = S.IN_CITY_LIMIT,
T.UM_SEC_ACCOUNT = S.UM_SEC_ACCOUNT,
T.JOBID = S.JOBID,
T.SUPERVISOR_NAME = S.SUPERVISOR_NAME,
T.START_DATE = S.START_DATE,
T.END_DATE = S.END_DATE,
T.UM_EXEMPT = S.UM_EXEMPT,
T.UM_THIRD_ACCOUNT = S.UM_THIRD_ACCOUNT,
T.UM_FOURTH_ACCOUNT = S.UM_FOURTH_ACCOUNT,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.EFF_STATUS <> S.EFF_STATUS or
T.EMPL_RCD <> S.EMPL_RCD or
T.AWARD_STATUS <> S.AWARD_STATUS or
nvl(trim(T.ACTION_DT),0) <> nvl(trim(S.ACTION_DT),0) or
T.WS_PLACEMENT_STAT <> S.WS_PLACEMENT_STAT or
nvl(trim(T.WS_PLACEMENT_DT),0) <> nvl(trim(S.WS_PLACEMENT_DT),0) or
T.COMMUNITY_SERVICE <> S.COMMUNITY_SERVICE or
nvl(trim(T.JOB_REC_EFFDT),0) <> nvl(trim(S.JOB_REC_EFFDT),0) or
T.JOB_REC_EFFSEQ <> S.JOB_REC_EFFSEQ or
T.EMPLOYER <> S.EMPLOYER or
T.PHONE <> S.PHONE or
T.DEPTID <> S.DEPTID or
T.LOCATION <> S.LOCATION or
T.EMAILID <> S.EMAILID or
T.HOURLY_RT <> S.HOURLY_RT or
T.ACCOUNT <> S.ACCOUNT or
T.PROFILE_ID <> S.PROFILE_ID or
nvl(trim(T.END_DT),0) <> nvl(trim(S.END_DT),0) or
T.COMMENTS_MSGS <> S.COMMENTS_MSGS or
T.COUNTRY <> S.COUNTRY or
T.ADDRESS1 <> S.ADDRESS1 or
T.ADDRESS2 <> S.ADDRESS2 or
T.ADDRESS3 <> S.ADDRESS3 or
T.ADDRESS4 <> S.ADDRESS4 or
T.CITY <> S.CITY or
T.NUM1 <> S.NUM1 or
T.NUM2 <> S.NUM2 or
T.HOUSE_TYPE <> S.HOUSE_TYPE or
T.ADDR_FIELD1 <> S.ADDR_FIELD1 or
T.ADDR_FIELD2 <> S.ADDR_FIELD2 or
T.ADDR_FIELD3 <> S.ADDR_FIELD3 or
T.COUNTY <> S.COUNTY or
T.STATE <> S.STATE or
T.POSTAL <> S.POSTAL or
T.GEO_CODE <> S.GEO_CODE or
T.IN_CITY_LIMIT <> S.IN_CITY_LIMIT or
T.UM_SEC_ACCOUNT <> S.UM_SEC_ACCOUNT or
T.JOBID <> S.JOBID or
T.SUPERVISOR_NAME <> S.SUPERVISOR_NAME or
nvl(trim(T.START_DATE),0) <> nvl(trim(S.START_DATE),0) or
nvl(trim(T.END_DATE),0) <> nvl(trim(S.END_DATE),0) or
T.UM_EXEMPT <> S.UM_EXEMPT or
T.UM_THIRD_ACCOUNT <> S.UM_THIRD_ACCOUNT or
T.UM_FOURTH_ACCOUNT <> S.UM_FOURTH_ACCOUNT or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.EMPLID,
T.INSTITUTION,
T.AID_YEAR,
T.ITEM_TYPE,
T.ACAD_CAREER,
T.SEQNO,
T.SRC_SYS_ID,
T.EFFDT,
T.EFF_STATUS,
T.EMPL_RCD,
T.AWARD_STATUS,
T.ACTION_DT,
T.WS_PLACEMENT_STAT,
T.WS_PLACEMENT_DT,
T.COMMUNITY_SERVICE,
T.JOB_REC_EFFDT,
T.JOB_REC_EFFSEQ,
T.EMPLOYER,
T.PHONE,
T.DEPTID,
T.LOCATION,
T.EMAILID,
T.HOURLY_RT,
T.ACCOUNT,
T.PROFILE_ID,
T.END_DT,
T.COMMENTS_MSGS,
T.COUNTRY,
T.ADDRESS1,
T.ADDRESS2,
T.ADDRESS3,
T.ADDRESS4,
T.CITY,
T.NUM1,
T.NUM2,
T.HOUSE_TYPE,
T.ADDR_FIELD1,
T.ADDR_FIELD2,
T.ADDR_FIELD3,
T.COUNTY,
T.STATE,
T.POSTAL,
T.GEO_CODE,
T.IN_CITY_LIMIT,
T.UM_SEC_ACCOUNT,
T.JOBID,
T.SUPERVISOR_NAME,
T.START_DATE,
T.END_DATE,
T.UM_EXEMPT,
T.UM_THIRD_ACCOUNT,
T.UM_FOURTH_ACCOUNT,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID
)
values (
S.EMPLID,
S.INSTITUTION,
S.AID_YEAR,
S.ITEM_TYPE,
S.ACAD_CAREER,
S.SEQNO,
'CS90',
S.EFFDT,
S.EFF_STATUS,
S.EMPL_RCD,
S.AWARD_STATUS,
S.ACTION_DT,
S.WS_PLACEMENT_STAT,
S.WS_PLACEMENT_DT,
S.COMMUNITY_SERVICE,
S.JOB_REC_EFFDT,
S.JOB_REC_EFFSEQ,
S.EMPLOYER,
S.PHONE,
S.DEPTID,
S.LOCATION,
S.EMAILID,
S.HOURLY_RT,
S.ACCOUNT,
S.PROFILE_ID,
S.END_DT,
S.COMMENTS_MSGS,
S.COUNTRY,
S.ADDRESS1,
S.ADDRESS2,
S.ADDRESS3,
S.ADDRESS4,
S.CITY,
S.NUM1,
S.NUM2,
S.HOUSE_TYPE,
S.ADDR_FIELD1,
S.ADDR_FIELD2,
S.ADDR_FIELD3,
S.COUNTY,
S.STATE,
S.POSTAL,
S.GEO_CODE,
S.IN_CITY_LIMIT,
S.UM_SEC_ACCOUNT,
S.JOBID,
S.SUPERVISOR_NAME,
S.START_DATE,
S.END_DATE,
S.UM_EXEMPT,
S.UM_THIRD_ACCOUNT,
S.UM_FOURTH_ACCOUNT,
'N',
'S',
sysdate,
sysdate,
1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_UM_STDNT_WS_AWD rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_UM_STDNT_WS_AWD',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_UM_STDNT_WS_AWD';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_UM_STDNT_WS_AWD';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_UM_STDNT_WS_AWD';
update CSSTG_OWNER.PS_UM_STDNT_WS_AWD T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, INSTITUTION, AID_YEAR, ITEM_TYPE, ACAD_CAREER, SEQNO, EFFDT
   from CSSTG_OWNER.PS_UM_STDNT_WS_AWD T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_STDNT_WS_AWD') = 'Y'
  minus
 select EMPLID, INSTITUTION, AID_YEAR, ITEM_TYPE, ACAD_CAREER, SEQNO, EFFDT
   from SYSADM.PS_UM_STDNT_WS_AWD@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_STDNT_WS_AWD') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.INSTITUTION = S.INSTITUTION
   and T.AID_YEAR = S.AID_YEAR
   and T.ITEM_TYPE = S.ITEM_TYPE
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.SEQNO = S.SEQNO
   and T.EFFDT = S.EFFDT    -- May 2017 
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_UM_STDNT_WS_AWD rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_UM_STDNT_WS_AWD',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_UM_STDNT_WS_AWD'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


EXCEPTION
    WHEN OTHERS THEN
        numSqlCode := SQLCODE;
        strSqlErrm := SQLERRM;

        ROLLBACK;
  
        strMessage01 := 'Error code: ' || TO_CHAR(SQLCODE) || ' Error Message: ' || SQLERRM;
        strMessage02 := TO_CHAR(SQLCODE);
  
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_FAILURE
                       (i_SqlCommand    => strSqlCommand,
                        i_ErrorText     => strMessage01,
                        i_ErrorCode     => strMessage02,
                        i_ErrorMessage  => strSqlErrm
                       );
               
        strMessage01 := 'Error...'
                        || strNewLine   || 'SQL Command:   ' || strSqlCommand
                        || strNewLine   || 'Error code:    ' || numSqlCode
                        || strNewLine   || 'Error Message: ' || strSqlErrm;

        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
        RAISE_APPLICATION_ERROR( -20001, strMessage01);

END PS_UM_STDNT_WS_AWD_P;
/
