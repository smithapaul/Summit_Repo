CREATE OR REPLACE PROCEDURE             "BB_TERM_S2_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- Jim Doucette
--
-- Loads S2 table BB_TERM_S2 from S1 table BB_TERM_S1.
--
-- V01  CASE-xxxxx 08/31/2020,    Jim Doucette
--                                New
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'BB_TERM_S2';
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

strMessage01    := 'Merging data into CSSTG_OWNER.BB_TERM_S2';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.BB_TERM_S2';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.BB_TERM_S2 T
using (select /*+ full(S) */
       BB_SOURCE, 
	   PK1, 
	   NAME, 
	   DESCRIPTION_FORMAT_TYPE, 
	   SOURCEDID_SOURCE, 
	   SOURCEDID_ID, 
	   DATA_SRC_PK1, 
	   DURATION, 
	   START_DATE, 
	   END_DATE, 
	   DAYS_OF_USE, 
	   ROW_STATUS, 
	   AVAILABLE_IND, 
	   DTMODIFIED, 
	   DELETE_FLAG, 
	   INSERT_TIME, 
	   UPDATE_TIME
  from CSSTG_OWNER.BB_TERM_S1) S
 on ( 
    T.BB_SOURCE = S.BB_SOURCE and 
    T.PK1 = S.PK1)
 when matched then update set
    T.NAME = S.NAME,
	T.DESCRIPTION_FORMAT_TYPE = S.DESCRIPTION_FORMAT_TYPE,
	T.SOURCEDID_SOURCE = S.SOURCEDID_SOURCE,
	T.SOURCEDID_ID = S.SOURCEDID_ID,
	T.DATA_SRC_PK1 = S.DATA_SRC_PK1,
	T.DURATION = S.DURATION,
	T.START_DATE = S.START_DATE,
	T.END_DATE = S.END_DATE,
	T.DAYS_OF_USE = S.DAYS_OF_USE,
	T.ROW_STATUS = S.ROW_STATUS,
	T.AVAILABLE_IND = S.AVAILABLE_IND,
	T.DTMODIFIED = S.DTMODIFIED,
	T.DELETE_FLAG = 'N',
	T.UPDATE_TIME = SYSDATE
where 
    nvl(trim(NAME),0) <> nvl(trim(S.NAME),0) or
    nvl(trim(DESCRIPTION_FORMAT_TYPE),0) <> nvl(trim(S.DESCRIPTION_FORMAT_TYPE),0) or
    nvl(trim(SOURCEDID_SOURCE),0) <> nvl(trim(S.SOURCEDID_SOURCE),0) or
    nvl(trim(SOURCEDID_ID),0) <> nvl(trim(S.SOURCEDID_ID),0) or
    nvl(trim(DATA_SRC_PK1),0) <> nvl(trim(S.DATA_SRC_PK1),0) or
    nvl(trim(DURATION),0) <> nvl(trim(S.DURATION),0) or
    nvl(trim(START_DATE),0) <> nvl(trim(S.START_DATE),0) or
    nvl(trim(END_DATE),0) <> nvl(trim(S.END_DATE),0) or
	nvl(trim(DAYS_OF_USE),0) <> nvl(trim(S.DAYS_OF_USE),0) or
	nvl(trim(ROW_STATUS),0) <> nvl(trim(S.ROW_STATUS),0) or
	nvl(trim(AVAILABLE_IND),0) <> nvl(trim(S.AVAILABLE_IND),0) or
    nvl(trim(DELETE_FLAG),0) <> nvl(trim('N'),0) or
    nvl(trim(UPDATE_TIME),0) <> nvl(trim(SYSDATE),0)
when not matched then 
insert (
    T.BB_SOURCE, 
    T.PK1, 
    T.NAME, 
    T.DESCRIPTION_FORMAT_TYPE, 
    T.SOURCEDID_SOURCE, 
    T.SOURCEDID_ID, 
    T.DATA_SRC_PK1, 
    T.DURATION, 
    T.START_DATE, 
    T.END_DATE, 
	T.DAYS_OF_USE, 
	T.ROW_STATUS, 
	T.AVAILABLE_IND, 
    T.DELETE_FLAG, 
    T.INSERT_TIME, 
    T.UPDATE_TIME  
) 
values (
    S.BB_SOURCE, 
    S.PK1, 
    S.NAME, 
    S.DESCRIPTION_FORMAT_TYPE, 
    S.SOURCEDID_SOURCE, 
    S.SOURCEDID_ID, 
    S.DATA_SRC_PK1, 
    S.DURATION, 
    S.START_DATE, 
    S.END_DATE, 
	S.DAYS_OF_USE, 
	S.ROW_STATUS, 
	S.AVAILABLE_IND,  
    'N',          
    SYSDATE,          
    SYSDATE)
	;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of BB_TERM_S2 rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'BB_TERM_S2',
                i_Action            => 'MERGE',
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

END BB_TERM_S2_P;
/
