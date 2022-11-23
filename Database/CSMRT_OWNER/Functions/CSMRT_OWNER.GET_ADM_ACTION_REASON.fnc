DROP FUNCTION CSMRT_OWNER.GET_ADM_ACTION_REASON
/

--
-- GET_ADM_ACTION_REASON  (Function) 
--
CREATE OR REPLACE FUNCTION CSMRT_OWNER.GET_ADM_ACTION_REASON
(
IN_PERSON_ID IN VARCHAR2, 
IN_ADM_APPL_NUM IN VARCHAR2, 
IN_ACTION_TYPE IN VARCHAR2, 
IN_MIN_MAX_FLAG IN VARCHAR2, 
IN_LATEST_TERM_FLAG IN VARCHAR2,
IN_CD_OR_DESC IN VARCHAR2 ) 
RETURN VARCHAR2 IS RET_REASON VARCHAR2(50);
BEGIN
      SELECT CASE WHEN IN_LATEST_TERM_FLAG = 'Y' THEN
                  CASE WHEN IN_MIN_MAX_FLAG = 'MAX' THEN 
                       CASE WHEN IN_CD_OR_DESC = 'CODE' THEN 
                              SUBSTR(MAX_TERM_MAX_RSN_CD, 1, 4)
                            ELSE 
                              SUBSTR(MAX_TERM_MAX_RSN_CD, 5)
                       END   
                       ELSE
                       CASE WHEN IN_CD_OR_DESC = 'CODE' THEN 
                              SUBSTR(MAX_TERM_MIN_RSN_CD, 1, 4)
                            ELSE 
                              SUBSTR(MAX_TERM_MIN_RSN_CD, 5)
                       END 
                  END
                  ELSE
                       CASE WHEN IN_MIN_MAX_FLAG = 'MAX' THEN 
                            CASE WHEN IN_CD_OR_DESC = 'CODE' THEN 
                              SUBSTR(MAX_RSN_CD, 1, 4)
                            ELSE 
                              SUBSTR(MAX_RSN_CD, 5)
                       END
                       ELSE
                       CASE WHEN IN_CD_OR_DESC = 'CODE' THEN 
                              SUBSTR(MIN_RSN_CD, 1, 4)
                            ELSE 
                              SUBSTR(MIN_RSN_CD, 5)
                       END 
                  END
             END
      INTO RET_REASON
      FROM UM_F_ADM_APPL_ACTION                
      WHERE PERSON_ID = IN_PERSON_ID
        AND ACN_RSN_KEY = 'PROG_ACN_CD'
        AND ACN_RSN_VAL = IN_ACTION_TYPE
        AND ADM_APPL_NBR = IN_ADM_APPL_NUM;
  RETURN RET_REASON;        
END GET_ADM_ACTION_REASON;
/
