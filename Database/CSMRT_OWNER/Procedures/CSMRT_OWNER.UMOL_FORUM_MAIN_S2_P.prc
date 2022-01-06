CREATE OR REPLACE PROCEDURE             "UMOL_FORUM_MAIN_S2_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- Jim Doucette
--
-- Loads S2 table UMOL_FORUM_MAIN_S2 from S1 table UMOL_FORUM_MAIN_S1.
--
-- V01  CASE-xxxxx 08/31/2020,    Jim Doucette
--                                New
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UMOL_FORUM_MAIN_S2';
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

strMessage01    := 'Merging data into CSSTG_OWNER.UMOL_FORUM_MAIN_S2';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.UMOL_FORUM_MAIN_S2';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.UMOL_FORUM_MAIN_S2 T
using (select /*+ full(S) */
        BB_SOURCE, 
		PK1, 
		DTCREATED, 
		DTMODIFIED, 
		CONFMAIN_PK1, 
		TEXT_FORMAT_TYPE, 
		ORDER_NUM, 
		NAME, 
		AVAILABLE_IND, 
		POST_FIRST, 
		START_DATE, 
		END_DATE, 
		UUID, 
		DELETE_FLAG, 
		INSERT_TIME, 
		UPDATE_TIME
  from CSSTG_OWNER.UMOL_FORUM_MAIN_S1) S
 on ( 
    T.BB_SOURCE = S.BB_SOURCE and 
    T.PK1 = S.PK1)
 when matched then update set
    T.DTCREATED = S.DTCREATED,
    T.DTMODIFIED = S.DTMODIFIED,
	T.CONFMAIN_PK1 = S.CONFMAIN_PK1,
	T.TEXT_FORMAT_TYPE = S.TEXT_FORMAT_TYPE,
	T.ORDER_NUM = S.ORDER_NUM,
	T.NAME = S.NAME,
	T.AVAILABLE_IND = S.AVAILABLE_IND,
	T.POST_FIRST = S.POST_FIRST,
	T.START_DATE = S.START_DATE,
	T.END_DATE = S.END_DATE,
	T.UUID = S.UUID,
	T.DELETE_FLAG = 'N',
	T.UPDATE_TIME = SYSDATE
where 
    nvl(trim(DTCREATED),0) <> nvl(trim(S.DTCREATED),0) or
	nvl(trim(DTMODIFIED),0) <> nvl(trim(S.DTMODIFIED),0) or
	nvl(trim(CONFMAIN_PK1),0) <> nvl(trim(S.CONFMAIN_PK1),0) or
	nvl(trim(TEXT_FORMAT_TYPE),0) <> nvl(trim(S.TEXT_FORMAT_TYPE),0) or
	nvl(trim(ORDER_NUM),0) <> nvl(trim(S.ORDER_NUM),0) or
	nvl(trim(NAME),0) <> nvl(trim(S.NAME),0) or
	nvl(trim(AVAILABLE_IND),0) <> nvl(trim(S.AVAILABLE_IND),0) or
	nvl(trim(POST_FIRST),0) <> nvl(trim(S.POST_FIRST),0) or
	nvl(trim(START_DATE),0) <> nvl(trim(S.START_DATE),0) or
	nvl(trim(END_DATE),0) <> nvl(trim(S.END_DATE),0) or
    nvl(trim(UUID),0) <> nvl(trim(S.UUID),0)
when not matched then 
insert (
    T.BB_SOURCE, 
    T.PK1, 
    T.DTCREATED, 
    T.DTMODIFIED, 
	T.CONFMAIN_PK1,
	T.TEXT_FORMAT_TYPE,
	T.ORDER_NUM,
	T.NAME,
	T.AVAILABLE_IND,
	T.POST_FIRST,
	T.START_DATE,
	T.END_DATE,
	T.UUID,
    T.DELETE_FLAG, 
    T.INSERT_TIME, 
    T.UPDATE_TIME  
) 
values (
    S.BB_SOURCE, 
    S.PK1, 
    S.DTCREATED, 
    S.DTMODIFIED, 
	S.CONFMAIN_PK1,
	S.TEXT_FORMAT_TYPE,
	S.ORDER_NUM,
	S.NAME,
	S.AVAILABLE_IND,
	S.POST_FIRST,
	S.START_DATE,
	S.END_DATE,
	S.UUID,
    'N',          
    SYSDATE,          
    SYSDATE)
	;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UMOL_FORUM_MAIN_S2 rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UMOL_FORUM_MAIN_S2',
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

END UMOL_FORUM_MAIN_S2_P;
/
