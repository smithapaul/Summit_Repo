DROP PROCEDURE CSMRT_OWNER.PS_S_SF_ACCTG_LN_P
/

--
-- PS_S_SF_ACCTG_LN_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_S_SF_ACCTG_LN_P" (
    i_FY           IN VARCHAR2,
    i_Replace_FY   IN VARCHAR2 DEFAULT 'N')
    AUTHID CURRENT_USER
IS
    ------------------------------------------------------------------------
    -- The purpose of this procedure is to create snapshot table based on fact table PS_S_SF_ACCTG_LN using fiscal year parameters
    -- i_FY is the year input for which snapshot is run . This year value is the field value for UM_fISCAL_YEAR
    -- i_Replace_Fy - if we have to replace existing snapshot data ,we need to change the variable to Y else default is N. When change to Y , 
    -- it will truncate the partition and reload the data into existing named partition

    ------------------------------------------------------------------------

    strMartId              VARCHAR2 (50) := 'CSMRT';
    strProcessName         VARCHAR2 (100) := 'PS_S_SF_ACCTG_LN_P';
    intProcessSid          INTEGER;
    dtProcessStart         DATE := SYSDATE;
    strMessage01           VARCHAR2 (4000);
    strMessage02           VARCHAR2 (512);
    strMessage03           VARCHAR2 (512) := '';
    strNewLine             VARCHAR2 (2) := CHR (13) || CHR (10);
    strSqlCommand          VARCHAR2 (32767) := '';
    strSqlDynamic          VARCHAR2 (32767) := '';
    V_REF_CUR_QUERY        VARCHAR2 (32767) := '';
    V_REF_CUR_DROP_QUERY   VARCHAR2 (32767) := '';
    strPartitionName       VARCHAR2 (100);
    strPartitionExists     VARCHAR2 (1);
    strPartitionCounter    INTEGER;
    strPartitionCounterflg VARCHAR2 (1);
    stroldPartitionYear     VARCHAR2 (10);
    stroldPartitionYearflg  VARCHAR2 (1);
    strClientInfo          VARCHAR2 (100);
    intRowCount            INTEGER := 0;
    intTotalRowCount       INTEGER := 0;
    numSqlCode             NUMBER;
    strSqlErrm             VARCHAR2 (4000);
    intTries               INTEGER;
    StrReplaceParition     VARCHAR2 (1);
    StrRecreateParition    VARCHAR2 (1);
BEGIN
    strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
    DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

    strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_INIT';
    COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT (
        i_MartId             => strMartId,
        i_ProcessName        => strProcessName,
        i_ProcessStartTime   => dtProcessStart,
        o_ProcessSid         => intProcessSid);

    strPartitionName := SUBSTR (i_FY, 4);
    StrReplaceParition := i_Replace_FY;
    DBMS_OUTPUT.PUT_LINE (
        'Partition name for field UM_FISCAL_YEAR is : ' || strPartitionName);

    strSqlCommand := 'SELECT strPartitionExists strPartitionCounter stroldParitionYear';
    
    
----this sql will provides information about existing partitions,how many years of partitions exists,and point out old partition if beyond 3 years of partition data exists       
SELECT  CASE WHEN PARTITION_COUNT >= 3 THEN 'Y' ELSE 'N' END PARTITION_COUNT_FLAG,
          case when  PARTITION_COUNT is null then 0 else PARTITION_COUNT end PARTITION_COUNT,
          case when OLD_PARTITION_YEAR is null then 'N' else 'Y' END OLD_PARTITION_YEAR_FLAG,
          OLD_PARTITION_YEAR,
        CASE WHEN  PARTITION_NAME IS NULL THEN  'N' ELSE  'Y'   END PARTITION_EXISTS
  INTO  strPartitionCounterflg,
        strPartitionCounter,
        stroldPartitionYearflg,
        stroldPartitionYear,
        strPartitionExists
  FROM  (
        SELECT  (
                
select COUNT(*) FROM (
SELECT PRT.PARTITION_NAME  ,to_number(SUBSTR(PRT.PARTITION_NAME,4)) PARTITION_YEAR,
ROW_NUMBER() OVER (ORDER BY to_number(SUBSTR(PRT.PARTITION_NAME,4)) ASC) row_id
  FROM ALL_TAB_PARTITIONS PRT
WHERE     PRT.TABLE_OWNER = 'CSMRT_OWNER'
AND PRT.TABLE_NAME = 'PS_S_SF_ACCTG_LN')
                )                   PARTITION_COUNT
        FROM  DUAL
        ),
        (
        SELECT  (
SELECT PRT.PARTITION_NAME
                      FROM ALL_TAB_PARTITIONS PRT
                     WHERE     PRT.TABLE_OWNER = 'CSMRT_OWNER'
                           AND PRT.TABLE_NAME = 'PS_S_SF_ACCTG_LN'
                           AND PRT.PARTITION_NAME = i_FY
                )                                       PARTITION_NAME
        FROM  DUAL
        ),
   ( select (
select PARTITION_NAME from (
SELECT PRT.PARTITION_NAME  ,to_number(SUBSTR(PRT.PARTITION_NAME,4)) OLD_PARTITION_YEAR,
ROW_NUMBER() OVER (ORDER BY to_number(SUBSTR(PRT.PARTITION_NAME,4)) ASC) row_id
  FROM ALL_TAB_PARTITIONS PRT
WHERE     PRT.TABLE_OWNER = 'CSMRT_OWNER'
AND PRT.TABLE_NAME = 'PS_S_SF_ACCTG_LN' ) where row_id =1) OLD_PARTITION_YEAR 
 FROM DUAL
 )
 ;
  DBMS_OUTPUT.PUT_LINE (
        'strPartitionCounterflg: ' || strPartitionCounterflg || ',' ||
        'strPartitionCounter: ' || strPartitionCounter || ',' ||
        'stroldPartitionYearflg: ' || stroldPartitionYearflg || ',' ||
        'stroldPartitionYear: ' || stroldPartitionYear || ',' || 
        'strPartitionExists: ' || strPartitionExists 
        );   
    
/*
    SELECT CASE WHEN PARTITION_NAME IS NULL THEN 'N' ELSE 'Y' END PARTITION_EXISTS
      INTO strPartitionExists
      FROM (SELECT (SELECT PRT.PARTITION_NAME
                      FROM ALL_TAB_PARTITIONS PRT
                     WHERE     PRT.TABLE_OWNER = 'CSMRT_OWNER'
                           AND PRT.TABLE_NAME = 'PS_S_SF_ACCTG_LN'
                           AND PRT.PARTITION_NAME = i_FY)    PARTITION_NAME
              FROM DUAL);
*/
 strSqlCommand := 'Validate Partition';                                           -----validate if partition exists and raise excep. as a error message
    IF strPartitionExists = 'Y' AND StrReplaceParition = 'N'
    THEN
        RAISE_APPLICATION_ERROR (
            -20001,
            'Partition ' || strPartitionName || ' already exists on the table');
    END IF;


strSqlCommand := 'Validate Partitioncounter and drop Older Partition';             -----validate if table has more than 3 yrs of partitions and drop the oldest partition
   IF strPartitionCounterflg = 'Y' and strPartitionExists='N'
    THEN
     strSqlDynamic :=
               'ALTER TABLE CSMRT_OWNER.PS_S_SF_ACCTG_LN DROP PARTITION '
            || stroldPartitionYear
            || ' UPDATE INDEXES';
        strSqlCommand := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
        COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE (
            i_SqlStatement   => strSqlDynamic,
            i_MaxTries       => 10,
            i_WaitSeconds    => 10,
            o_Tries          => intTries);
    END IF;


    IF strPartitionExists = 'Y' AND StrReplaceParition = 'Y'                       ---validate if table already has partition with input fiscal year and input parameter is 'Y' for i_replace_FY and truncate the partition to reload the data
    THEN
        strSqlDynamic :=
               'ALTER TABLE CSMRT_OWNER.PS_S_SF_ACCTG_LN TRUNCATE PARTITION '
            || i_FY
            || ' UPDATE INDEXES';
        strSqlCommand := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
        COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE (
            i_SqlStatement   => strSqlDynamic,
            i_MaxTries       => 10,
            i_WaitSeconds    => 10,
            o_Tries          => intTries);
    END IF;



    IF strPartitionExists = 'N'
    THEN
        V_REF_CUR_QUERY :=
               'ALTER TABLE CSMRT_OWNER.PS_S_SF_ACCTG_LN ADD PARTITION '
            || i_FY
            || ' values'
            || '('
            || strPartitionName
            || ')';

        EXECUTE IMMEDIATE V_REF_CUR_QUERY;

        DBMS_OUTPUT.PUT_LINE (V_REF_CUR_QUERY);
        DBMS_OUTPUT.PUT_LINE ('RAN SUCCESSFULLY');
        DBMS_OUTPUT.PUT_LINE (
            'VALUE OF V_REF_CUR_QUERY ' || StrRecreateParition);
    END IF;

    strMessage01 :=
        'Disabling Indexes for table CSMRT_OWNER.PS_S_SF_ACCTG_LN';
    COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);
    COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE ('CSMRT_OWNER', 'PS_S_SF_ACCTG_LN');
    DBMS_OUTPUT.PUT_LINE (strMessage01);

    strSqlDynamic :=
        'alter table CSMRT_OWNER.PS_S_SF_ACCTG_LN disable constraint PK_PS_S_SF_ACCTG_LN';
    strSqlCommand := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
    COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE (
        i_SqlStatement   => strSqlDynamic,
        i_MaxTries       => 10,
        i_WaitSeconds    => 10,
        o_Tries          => intTries);
    DBMS_OUTPUT.PUT_LINE (strSqlDynamic);

    strMessage01 := 'Inserting into CSMRT_OWNER.PS_S_SF_ACCTG_LN';
    DBMS_OUTPUT.PUT_LINE (strMessage01);

    INSERT /*+ append parallel(8)*/
           INTO CSMRT_OWNER.PS_S_SF_ACCTG_LN
    SELECT strPartitionName AS  UM_FISCAL_YEAR,F1.*, sysdate as UM_SNAPSHOT_DT from CSSTG_OWNER.PS_SF_ACCTG_LN F1  
 
    ;

    strSqlCommand := 'SET intRowCount';
    intRowCount := SQL%ROWCOUNT;

    strSqlCommand := 'commit';
    COMMIT;

    strMessage01 :=
           '# of PS_S_SF_ACCTG_LN rows inserted: '
        || TO_CHAR (intRowCount, '999,999,999,999');
    COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

    strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
    COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
        i_TargetTableName   => 'PS_S_SF_ACCTG_LN',
        i_Action            => 'INSERT',
        i_RowCount          => intRowCount);


    strMessage01 :=
        'Enabling Indexes for table CSMRT_OWNER.PS_S_SF_ACCTG_LN';
    COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

    strSqlDynamic :=
        'alter table CSMRT_OWNER.PS_S_SF_ACCTG_LN enable constraint PK_PS_S_SF_ACCTG_LN';
    strSqlCommand := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
    COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE (
        i_SqlStatement   => strSqlDynamic,
        i_MaxTries       => 10,
        i_WaitSeconds    => 10,
        o_Tries          => intTries);

    strMessage01 :=
        'Rebuilding Indexes for table CSMRT_OWNER.PS_S_SF_ACCTG_LN';
    COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);
    COMMON_OWNER.SMT_INDEX.ALL_REBUILD ('CSMRT_OWNER', 'PS_S_SF_ACCTG_LN');


    strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
    COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

    strMessage01 := strProcessName || ' is complete.';
    COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

    DBMS_OUTPUT.PUT_LINE ('end of the procedure');


EXCEPTION
    WHEN OTHERS
    THEN
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION (
            i_SqlCommand   => strSqlCommand,
            i_SqlCode      => SQLCODE,
            i_SqlErrm      => SQLERRM);
END PS_S_SF_ACCTG_LN_P;
/
