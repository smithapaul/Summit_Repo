CREATE OR REPLACE PROCEDURE             "UM_F_FA_STDNT_AWARDS_MV_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Refresh Materialized view  -- UM_F_FA_STDNT_AWARDS_MV
--V01 12/14/2018             -- srikanth ,pabbu converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_FA_STDNT_AWARDS_MV';
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

strMessage01    := 'Preparing Materialized view CSMRT_OWNER.UM_F_FA_STDNT_AWARDS_MV';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

EXECUTE IMMEDIATE 'ALTER MATERIALIZED VIEW UM_F_FA_STDNT_AWARDS_MV
                        PARALLEL 16
                    DISABLE QUERY REWRITE';

DBMS_MVIEW.REFRESH('UM_F_FA_STDNT_AWARDS_MV','C', ATOMIC_REFRESH => FALSE);			

EXECUTE IMMEDIATE 'ALTER MATERIALIZED VIEW UM_F_FA_STDNT_AWARDS_MV 
                        NOPARALLEL
                   ENABLE QUERY REWRITE';
                   
strSqlCommand := 'commit';
commit;

    strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_AWARDS_MV',
                i_Action            => 'Refresh MV',
                i_RowCount          => intRowCount
        );

DBMS_STATS.gather_table_stats(ownname => 'CSMRT_OWNER', tabname => 'UM_F_FA_STDNT_AWARDS_MV' , cascade => TRUE, degree => 16, estimate_percent => DBMS_STATS.auto_sample_size, method_opt => 'FOR ALL COLUMNS SIZE SKEWONLY' );

dbms_stats.set_table_stats(OWNNAME=>'CSMRT_OWNER', TABNAME=>'UM_F_FA_STDNT_AWARDS_MV', NUMROWS=>100000000);

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

END UM_F_FA_STDNT_AWARDS_MV_P;
/
