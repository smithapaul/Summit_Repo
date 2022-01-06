CREATE OR REPLACE PROCEDURE               "UM_AF_PRD_AMH_STDNT_ENRL_S2_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- James Doucette
--
-- Loads stage table CSSTG_OWNER.UM_AF_AMH_STUDENT_ENROLL_S2.
--
-- V01  SMT-xxxx 08/10/2018,    James Doucette
--                              Converted from VendorLocHighMatchS2.sql
--                              Intended to run in the Production environment only.
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_AF_PRD_AMH_STDNT_ENRL_S2_P';
        dtProcessStart                  Date            := SYSDATE;
        intProcessSid                   INTEGER;
        strMessage01                    VARCHAR2(4000);
        strMessage02                    VARCHAR2(512);
        strMessage03                    VARCHAR2(512)   :='';
        strNewLine                      VARCHAR2(2)     := chr(13) || chr(10);
        strSqlCommand                   VARCHAR2(32767) :='';
        strSqlDynamic                   VARCHAR2(32767) :='';
        strClientInfo                   VARCHAR2(100);
        intRowCount                     INTEGER;
        intTotalRowCount                INTEGER         := 0;
        numSqlCode                      NUMBER;
        strSqlErrm                      VARCHAR2(4000);
        intTries                        INTEGER;
        intYear                         INTEGER;
        strControlRowExists             VARCHAR2(1);
        intPartitionIndicatorCurrent    INTEGER;
        intPartitionIndicatorNew        INTEGER;
        strPartitionNameNew             VARCHAR2(30);
        strCarriageReturn               Varchar2(1) := chr(13);
        rtpTarget                       CSSTG_OWNER.UM_AF_AMH_STUDENT_ENROLL_S2%ROWTYPE; -- Creates a record with columns matching those in the target table
        intErrorCount                   Integer := 0;
        intRowNum                       Integer;
        intHeaderRowCount               Integer := 0;
        bolError                        Boolean;
        intInsertCount                  Integer := 0;
        intFailedRowCount               Integer := 0;
        intFailedRowMax                 Integer := 10;
        

BEGIN
strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
        (
                i_MartId                => strMartId,
                i_ProcessName           => strProcessName,
                i_ProcessStartTime      => dtProcessStart,
                o_ProcessSid            => intProcessSid
        );

strSqlCommand := 'SMT_INTERFACE.INTERFACE_INIT';
COMMON_OWNER.SMT_INTERFACE.INTERFACE_INIT
                (       i_SourceTableOwner      => 'CSSTG_OWNER',
                        i_SourceTableName       => 'UM_AF_AMH_STUDENT_ENROLL_EXT',
                        i_TargetTableOwner      => 'CSSTG_OWNER',
                        i_TargetTableName       => 'AF_AMH_STUDENT_ENROLL_S2'
                );

strSqlDynamic           := 'TRUNCATE TABLE CSSTG_OWNER.UM_AF_AMH_STUDENT_ENROLL_S2';
strSqlCommand           := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (       i_SqlStatement  => strSqlDynamic,
                        i_MaxTries      => 10,
                        i_WaitSeconds   => 10,
                        o_Tries         => intTries
                );

strMessage01 := ' Inserting UM_AF_AMH_STUDENT_ENROLL_S2 row from UM_AF_AMH_STUDENT_ENROLL_EXT...';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


-- Loop through the source external table which uses the interface file.
strSqlCommand           := 'SELECT FROM CSSTG_OWNER.UM_AF_AMH_STUDENT_ENROLL_EXT';
FOR EXTERNAL_REC IN
        (
        SELECT  ROWNUM,
		        INSTITUTION,
				INSTITUTION_NAME,
				TERM_CODE,
				TERM_DESCR,
				ACAD_YEAR,
				FISCAL_YEAR,
				EMPLID,
				ACAD_CAREER,
				ACAD_CAR_DESCR,
				CPE_FLAG,
				ACAD_PROG_PRIMARY,
				ACAD_PROG_DESCR,
				ACAD_PROG_CIP,
				ACAD_PROG_CIP_DESCR,
				PLAN_CODE,
				PLAN_DESCR,
				PLAN_CIP,
				PLAN_CIP_DESCR,
				NEW,
				RESIDENCY_CODE,
				RESIDENCY_DESCR,
				IN_STATE,
				ONLINE_ONLY,
				HYBRID,
				TOTAL_FTE,
				ONLINE_FTE,
				TOTAL_SCH,
				ONLINE_SCH,
				NON_ONLINE_SCH,
				CPE_SCH,
				NON_CPE_SCH,
				CRSE_COUNT,
				CRSE_COUNT_ONLINE,
				CRSE_COUNT_CPE,
				ACAD_ORG,           -- Sept 2018 
				ACAD_ORG_DESCR      -- Sept 2018 
          FROM  CSSTG_OWNER.UM_AF_AMH_STUDENT_ENROLL_EXT
        )
Loop
    strSqlCommand := '';
    If  UPPER(EXTERNAL_REC.INSTITUTION)          = 'INSTITUTION'
    Then
        intHeaderRowCount       := intHeaderRowCount + 1;
    Else        
        bolError        := False;
        intRowNum       := EXTERNAL_REC.rownum;
        
        strSqlCommand   := 'SMT_CONTEXT.SET_ATTRIBUTE';
        COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceRowNumber', i_AttributeValue=> to_char(intRowNum));
        

--      Move each source row's unformatted varchar2 column value to the corresponding column of
--      the record that matches the target table's columns doing appropriate transformation.
--      This allows individual trapping and reporting of transformation errors.
--      If there is an error SMT_INTERFACE.LOG_ERROR is called to record it and the process continues
--      to the next column.
        Begin
                rtpTarget.INSTITUTION_CD      := EXTERNAL_REC.INSTITUTION;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                ( 
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'INSTITUTION',
                                        i_ColumnValue           => EXTERNAL_REC.INSTITUTION
                                );
        End;
        Begin
                rtpTarget.INSTITUTION_LD      := trim(EXTERNAL_REC.INSTITUTION_NAME);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'INSTITUTION_NAME',
                                        i_ColumnValue           => EXTERNAL_REC.INSTITUTION_NAME
                                );
        End;
        Begin
                rtpTarget.TERM_CD := trim(EXTERNAL_REC.TERM_CODE);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (  
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'TERM_CODE',
                                        i_ColumnValue           => EXTERNAL_REC.TERM_CODE
                                );
        End;
        Begin
                rtpTarget.TERM_LD          := trim(EXTERNAL_REC.TERM_DESCR);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (   
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'TERM_DESCR',
                                        i_ColumnValue           => EXTERNAL_REC.TERM_DESCR
                                );
        End;
        Begin
                rtpTarget.ACAD_YR       := trim(EXTERNAL_REC.ACAD_YEAR);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (   
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'ACAD_YEAR',
                                        i_ColumnValue           => EXTERNAL_REC.ACAD_YEAR
                                );
        End;
        Begin
                rtpTarget.AID_YEAR       := trim(EXTERNAL_REC.FISCAL_YEAR);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (  
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'FISCAL_YEAR',
                                        i_ColumnValue           => EXTERNAL_REC.FISCAL_YEAR
                                );
        End;
        Begin
                rtpTarget.PERSON_ID           := trim(EXTERNAL_REC.EMPLID);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (  
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'EMPLID',
                                        i_ColumnValue           => EXTERNAL_REC.EMPLID
                                );
        End;
        Begin
                rtpTarget.ACAD_CAR_CD         := trim(EXTERNAL_REC.ACAD_CAREER);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (  
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'ACAD_CAREER',
                                        i_ColumnValue           => EXTERNAL_REC.ACAD_CAREER
                                );
        End;
        Begin
                rtpTarget.ACAD_CAR_LD        := trim(EXTERNAL_REC.ACAD_CAR_DESCR);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (  
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'ACAD_CAR_DESCR',
                                        i_ColumnValue           => EXTERNAL_REC.ACAD_CAR_DESCR
                                );
        End;
        Begin
                rtpTarget.CE_ONLY_FLG        := trim(EXTERNAL_REC.CPE_FLAG);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (  
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'CPE_FLAG',
                                        i_ColumnValue           => EXTERNAL_REC.CPE_FLAG
                                );
        End;
        Begin
                rtpTarget.ACAD_PROG_CD        := trim(EXTERNAL_REC.ACAD_PROG_PRIMARY);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (   
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'ACAD_PROG_PRIMARY',
                                        i_ColumnValue           => EXTERNAL_REC.ACAD_PROG_PRIMARY
                                );
        End;
        Begin
                rtpTarget.ACAD_PROG_LD        := trim(EXTERNAL_REC.ACAD_PROG_DESCR);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (  
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'ACAD_PROG_DESCR',
                                        i_ColumnValue           => EXTERNAL_REC.ACAD_PROG_DESCR
                                );
        End;
        Begin
                rtpTarget.PROG_CIP_CD        := trim(EXTERNAL_REC.ACAD_PROG_CIP);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (  
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'ACAD_PROG_CIP',
                                        i_ColumnValue           => EXTERNAL_REC.ACAD_PROG_CIP
                                );
        End;
        Begin
                rtpTarget.PROG_CIP_LD        := trim(EXTERNAL_REC.ACAD_PROG_CIP_DESCR);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (  
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'ACAD_PROG_CIP_DESCR',
                                        i_ColumnValue           => EXTERNAL_REC.ACAD_PROG_CIP_DESCR
                                );
        End;
        Begin
                rtpTarget.ACAD_PLAN_CD        := trim(EXTERNAL_REC.PLAN_CODE);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (  
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'PLAN_CODE',
                                        i_ColumnValue           => EXTERNAL_REC.PLAN_CODE
                                );
        End;
        Begin
                rtpTarget.ACAD_PLAN_LD        := trim(EXTERNAL_REC.PLAN_DESCR);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (   
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'PLAN_DESCR',
                                        i_ColumnValue           => EXTERNAL_REC.PLAN_DESCR
                                );
        End;
        Begin
                rtpTarget.PLAN_CIP_CD        := trim(EXTERNAL_REC.PLAN_CIP);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (   
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'PLAN_CIP',
                                        i_ColumnValue           => EXTERNAL_REC.PLAN_CIP
                                );
        End;
        Begin
                rtpTarget.PLAN_CIP_LD        := trim(EXTERNAL_REC.PLAN_CIP_DESCR);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (   
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'PLAN_CIP_DESCR',
                                        i_ColumnValue           => EXTERNAL_REC.PLAN_CIP_DESCR
                                );
        End;
        Begin
                rtpTarget.NEW_CONT_IND        := trim(EXTERNAL_REC.NEW);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (   
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'NEW',
                                        i_ColumnValue           => EXTERNAL_REC.NEW
                                );
        End;
        Begin
                rtpTarget.RSDNCY_ID        := trim(EXTERNAL_REC.RESIDENCY_CODE);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (  
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'RESIDENCY_CODE',
                                        i_ColumnValue           => EXTERNAL_REC.RESIDENCY_CODE
                                );
        End;
        Begin
                rtpTarget.RSDNCY_LD        := trim(EXTERNAL_REC.RESIDENCY_DESCR);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (  
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'RESIDENCY_DESCR',
                                        i_ColumnValue           => EXTERNAL_REC.RESIDENCY_DESCR
                                );
        End;
        Begin
                rtpTarget.IS_RSDNCY_FLG        := trim(EXTERNAL_REC.IN_STATE);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (   
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'IN_STATE',
                                        i_ColumnValue           => EXTERNAL_REC.IN_STATE
                                );
        End;
        Begin
                rtpTarget.ONLINE_ONLY_FLG        := trim(EXTERNAL_REC.ONLINE_ONLY);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (  
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'ONLINE_ONLY',
                                        i_ColumnValue           => EXTERNAL_REC.ONLINE_ONLY
                                );
        End;
        Begin
                rtpTarget.ONLINE_HYBRID_FLG        := trim(EXTERNAL_REC.HYBRID);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (    
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'HYBRID',
                                        i_ColumnValue           => EXTERNAL_REC.HYBRID
                                );
        End;
        Begin
                rtpTarget.TOT_FTE        := EXTERNAL_REC.TOTAL_FTE;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (  
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'TOTAL_FTE',
                                        i_ColumnValue           => EXTERNAL_REC.TOTAL_FTE
                                );
        End;
        Begin
                rtpTarget.ONLINE_FTE        := EXTERNAL_REC.ONLINE_FTE;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (     
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'ONLINE_FTE',
                                        i_ColumnValue           => EXTERNAL_REC.ONLINE_FTE
                                );
        End;
        Begin
                rtpTarget.TOT_CREDITS        := EXTERNAL_REC.TOTAL_SCH;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (  
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'TOTAL_SCH',
                                        i_ColumnValue           => EXTERNAL_REC.TOTAL_SCH
                                );
        End;
        Begin
                rtpTarget.ONLINE_CREDITS        := EXTERNAL_REC.ONLINE_SCH;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                ( 
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'ONLINE_SCH',
                                        i_ColumnValue           => EXTERNAL_REC.ONLINE_SCH
                                );
        End;
        Begin
                rtpTarget.NON_ONLINE_CREDITS        := EXTERNAL_REC.NON_ONLINE_SCH;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (    
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'NON_ONLINE_SCH',
                                        i_ColumnValue           => EXTERNAL_REC.NON_ONLINE_SCH
                                );
        End;
        Begin
                rtpTarget.CE_CREDITS        := EXTERNAL_REC.CPE_SCH;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (   
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'CPE_SCH',
                                        i_ColumnValue           => EXTERNAL_REC.CPE_SCH
                                );
        End;
        Begin
                rtpTarget.NON_CE_CREDITS        := EXTERNAL_REC.NON_CPE_SCH;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (     
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'NON_CPE_SCH',
                                        i_ColumnValue           => EXTERNAL_REC.NON_CPE_SCH
                                );
        End;
        Begin
                rtpTarget.ENROLL_CNT        := EXTERNAL_REC.CRSE_COUNT;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (  
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'CRSE_COUNT',
                                        i_ColumnValue           => EXTERNAL_REC.CRSE_COUNT
                                );
        End;
        Begin
                rtpTarget.ONLINE_CNT        := EXTERNAL_REC.CRSE_COUNT_ONLINE;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (    
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'CRSE_COUNT_ONLINE',
                                        i_ColumnValue           => EXTERNAL_REC.CRSE_COUNT_ONLINE
                                );
        End;
        Begin
                rtpTarget.CE_CNT        := EXTERNAL_REC.CRSE_COUNT_CPE;
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (  
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'CRSE_COUNT_CPE',
                                        i_ColumnValue           => EXTERNAL_REC.CRSE_COUNT_CPE
                                );
        End;
        Begin
                rtpTarget.ACAD_ORG_CD        := trim(EXTERNAL_REC.ACAD_ORG);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (   
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'ACAD_ORG',
                                        i_ColumnValue           => EXTERNAL_REC.ACAD_ORG
                                );
        End;
        Begin
                rtpTarget.ACAD_ORG_LD        := trim(EXTERNAL_REC.ACAD_ORG_DESCR);
                EXCEPTION WHEN OTHERS THEN
                        intErrorCount   := intErrorCount + 1;
                        bolError        := True;
                        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (  
                                        i_ErrorSequence         => intErrorCount,
                                        i_ErrorDescription      => 'Column transformation error',
                                        i_ErrorMessage          => SQLERRM,
                                        i_ErrorCode             => SQLCODE,
                                        i_ColumnName            => 'ACAD_ORG_DESCR',
                                        i_ColumnValue           => EXTERNAL_REC.ACAD_ORG_DESCR
                                );
        End;
--      If there were no column transformation errors then insert the target row. 
        If  not bolError
        Then
                Begin
                        INSERT  /*+APPEND*/
                          INTO  CSSTG_OWNER.UM_AF_AMH_STUDENT_ENROLL_S2
                                (
                                INSTITUTION_CD,
								INSTITUTION_LD,
								TERM_CD,
								TERM_LD,
								ACAD_YR,
								AID_YEAR,
								PERSON_ID,
								ACAD_CAR_CD,
								ACAD_CAR_LD,
								CE_ONLY_FLG,
								ACAD_ORG_CD,
								ACAD_ORG_LD,
								ACAD_PROG_CD,
								ACAD_PROG_LD,
								PROG_CIP_CD,
								PROG_CIP_LD,
								ACAD_PLAN_CD,
								ACAD_PLAN_LD,
								PLAN_CIP_CD,
								PLAN_CIP_LD,
								NEW_CONT_IND,
								RSDNCY_ID,
								RSDNCY_LD,
								IS_RSDNCY_FLG,
								ONLINE_ONLY_FLG,
								ONLINE_HYBRID_FLG,
								TOT_FTE,
								ONLINE_FTE,
								TOT_CREDITS,
								ONLINE_CREDITS,
								NON_ONLINE_CREDITS,
								CE_CREDITS,
								NON_CE_CREDITS,
								ENROLL_CNT,
								ONLINE_CNT,
								CE_CNT,
								CREATED_EW_DTTM
                                )
                        VALUES
                                (
                                rtpTarget.INSTITUTION_CD,
                                rtpTarget.INSTITUTION_LD,
                                rtpTarget.TERM_CD,
                                rtpTarget.TERM_LD,
                                rtpTarget.ACAD_YR,
                                rtpTarget.AID_YEAR,
                                rtpTarget.PERSON_ID,
                                rtpTarget.ACAD_CAR_CD,
                                rtpTarget.ACAD_CAR_LD,
                                rtpTarget.CE_ONLY_FLG,
                                rtpTarget.ACAD_ORG_CD,
                                rtpTarget.ACAD_ORG_LD,
                                rtpTarget.ACAD_PROG_CD,
                                rtpTarget.ACAD_PROG_LD,
                                rtpTarget.PROG_CIP_CD,
                                rtpTarget.PROG_CIP_LD,
                                rtpTarget.ACAD_PLAN_CD,
                                rtpTarget.ACAD_PLAN_LD,
                                rtpTarget.PLAN_CIP_CD,
                                rtpTarget.PLAN_CIP_LD,
                                rtpTarget.NEW_CONT_IND,
                                rtpTarget.RSDNCY_ID,
                                rtpTarget.RSDNCY_LD,
                                rtpTarget.IS_RSDNCY_FLG,
                                rtpTarget.ONLINE_ONLY_FLG,
                                rtpTarget.ONLINE_HYBRID_FLG,
                                rtpTarget.TOT_FTE,
                                rtpTarget.ONLINE_FTE,
                                rtpTarget.TOT_CREDITS,
                                rtpTarget.ONLINE_CREDITS,
                                rtpTarget.NON_ONLINE_CREDITS,
                                rtpTarget.CE_CREDITS,
                                rtpTarget.NON_CE_CREDITS,
                                rtpTarget.ENROLL_CNT,
                                rtpTarget.ONLINE_CNT,
								rtpTarget.CE_CNT,
                                SYSDATE
                                )
                        ;
        
                        EXCEPTION
                          WHEN OTHERS THEN
                                -- If we get here then the error should be something other than
                                -- a bad column value.  Unique constraint violation is one possibility.
                                intErrorCount   := intErrorCount + 1;
                                bolError        := True;
                                COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                        (    
                                                i_ErrorSequence         => intErrorCount,
                                                i_ErrorDescription      => 'Row insert error',
                                                i_ErrorMessage          => SQLERRM,
                                                i_ErrorCode             => SQLCODE
                                        );
                END;
        End If;

        If  bolError
        Then
                intFailedRowCount := intFailedRowCount + 1;
        Else
                intInsertCount    := intInsertCount + 1;
        End If;

        -- If the maximum number of failed rows is exceeded then stop getting source rows.
        If  intFailedRowCount > intFailedRowMax
        Then
                COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                                (  
                                        i_ErrorSequence         => intErrorCount + 1,
                                        i_ErrorDescription      => 'Termination before end of data',
                                        i_ErrorMessage          => 'Maximum number of failed rows exceeded',
                                        i_ErrorCode             => Null
                                );
                Exit;
        End If;

    End If;

End Loop;

strMessage01    := '# rows inserted: ' || TO_CHAR(intInsertCount);
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

If  intErrorCount = 0
Then
--      No errors, commit and mark the interface as completed.
        strSqlCommand   := 'COMMIT (UM_AF_AMH_STUDENT_ENROLL_S2 inserts)';
        COMMIT;

        strSqlCommand := 'SMT_INTERFACE.INTERFACE_SUCCESS';
        COMMON_OWNER.SMT_INTERFACE.INTERFACE_SUCCESS
                (       i_SuccessRowCount       => intInsertCount,
                        i_FailedRowCount        => intFailedRowCount
                );

        strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

        strMessage01    := strProcessName || ' is complete.';
        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

Else
--      There were one or more errors. Rollback and mark the interface as failed.
--      Do not cause the job to fail. The user will see the failure when checking the interface's status.
--      This avoids the complication of handling a failed Control-M job.
        strSqlCommand := 'SMT_INTERFACE.INTERFACE_FAILURE';
        COMMON_OWNER.SMT_INTERFACE.INTERFACE_FAILURE
                (       i_SuccessRowCount       => intInsertCount,
                        i_FailedRowCount        => intFailedRowCount
                );

        -- This rollback undoes any inserts into the target table.
        strSqlCommand   := 'ROLLBACK';
        ROLLBACK;

        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_FAILURE
                       (i_SqlCommand    => Null,
                        i_ErrorText     => 'Interface had one or more errors.  See INTERFACE_ERROR_LOG.',
                        i_ErrorCode     => Null,
                        i_ErrorMessage  => Null
                       );


End If;


EXCEPTION
    WHEN OTHERS THEN
        numSqlCode := SQLCODE;
        strSqlErrm := SQLERRM;
        
        intErrorCount   := intErrorCount + 1;
        bolError        := True;
        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                (       i_ErrorSequence         => intErrorCount,
                        i_ErrorDescription      => 'General error',
                        i_ErrorMessage          => strSqlErrm,
                        i_ErrorCode             => numSqlCode
                );

        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION
                (
                        i_SqlCommand   => strSqlCommand,
                        i_SqlCode      => numSqlCode,
                        i_SqlErrm      => strSqlErrm
                );


END UM_AF_PRD_AMH_STDNT_ENRL_S2_P;
/
