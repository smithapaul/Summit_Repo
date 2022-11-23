DROP FUNCTION CSMRT_OWNER.GET_AWARD_AMT_OLD
/

--
-- GET_AWARD_AMT_OLD  (Function) 
--
CREATE OR REPLACE FUNCTION CSMRT_OWNER.GET_AWARD_AMT_OLD
(
IN_PERSON_ID  VARCHAR2,
IN_INSTITUTION_CD  VARCHAR2,
IN_CAREER_CD  VARCHAR2,
IN_PERIOD_TYPE  VARCHAR2,
IN_PERIOD_TYPE_VAL  VARCHAR2,
IN_COLUMN_TYPE  VARCHAR2,
IN_COLUMN_TYPE_VAL  VARCHAR2,
IN_AMOUNT_TYPE   VARCHAR2)
RETURN NUMBER IS 
  RET_AMOUNT NUMBER(16,2);
  SQL_STR VARCHAR2(4000);
BEGIN

  SQL_STR :=            'SELECT SUM( ';
  SQL_STR := SQL_STR || 'CASE WHEN '  || '''' ||  IN_AMOUNT_TYPE  || '''' || ' = ' || '''OFFER_AMT''' || ' THEN DISB.OFFER_BALANCE ';
  SQL_STR := SQL_STR || '     WHEN '  || '''' ||  IN_AMOUNT_TYPE  || '''' || ' = ' || '''ACCEPT_AMT''' || ' THEN DISB.ACCEPT_BALANCE ';
  SQL_STR := SQL_STR || '     WHEN '  || '''' ||  IN_AMOUNT_TYPE  || '''' || ' = ' || '''DISB_AMT''' || ' THEN DISB.DISBURSED_BALANCE ';   
  SQL_STR := SQL_STR || '     ELSE NULL END ) RET_AMOUNT ';   
  
  SQL_STR := SQL_STR || 'FROM UM_D_FA_ITEM_TYPE FA_ITEM, ';
  SQL_STR := SQL_STR || '     PS_D_ITEM_TYPE ITEM, ';
  SQL_STR := SQL_STR || '     UM_F_FA_AWARD_DISB DISB ';
  
  SQL_STR := SQL_STR || 'WHERE DISB.PERSON_ID = ' || '''' || IN_PERSON_ID || '''';    
  SQL_STR := SQL_STR || '  AND DISB.INSTITUTION_CD = ' || '''' || IN_INSTITUTION_CD || '''';  
  SQL_STR := SQL_STR || '  AND DISB.ACAD_CAR_CD = ' || '''' || IN_CAREER_CD || '''';   
  SQL_STR := SQL_STR || '  AND DISB.ITEM_TYPE_SID = FA_ITEM.ITEM_TYPE_SID ';   
  SQL_STR := SQL_STR || '  AND FA_ITEM.INSTITUTION_CD = ITEM.SETID ';   
  SQL_STR := SQL_STR || '  AND FA_ITEM.ITEM_TYPE = ITEM.ITEM_TYPE_ID '; 
  SQL_STR := SQL_STR || '  AND FA_ITEM.SRC_SYS_ID = ITEM.SRC_SYS_ID '; 
  
  SQL_STR := SQL_STR || '  AND CASE WHEN '  || '''' ||  IN_PERIOD_TYPE  || '''' || ' = ' || '''AWARD_TERM''' || ' THEN DISB.TERM_CD ';     
  SQL_STR := SQL_STR || '           WHEN '  || '''' ||  IN_PERIOD_TYPE  || '''' || ' = ' || '''AID_YEAR''' || ' THEN DISB.AID_YEAR ';   
  SQL_STR := SQL_STR || '           ELSE NULL END IN (' || GET_MULTI_VAL_STRING(IN_PERIOD_TYPE_VAL) || ')';    

  SQL_STR := SQL_STR || '  AND CASE WHEN '  || '''' ||  IN_COLUMN_TYPE  || '''' || ' = ' || '''AGGREGATE_AREA''' || ' THEN FA_ITEM.AGGREGATE_AREA ';     
  SQL_STR := SQL_STR || '           WHEN '  || '''' ||  IN_COLUMN_TYPE  || '''' || ' = ' || '''FIN_AID_TYPE''' || ' THEN FA_ITEM.FIN_AID_TYPE_LD '; 
  SQL_STR := SQL_STR || '           WHEN '  || '''' ||  IN_COLUMN_TYPE  || '''' || ' = ' || '''FA_SOURCE''' || ' THEN FA_ITEM.FA_SOURCE '; 
  SQL_STR := SQL_STR || '           WHEN '  || '''' ||  IN_COLUMN_TYPE  || '''' || ' = ' || '''FA_ITEM_TYPE''' || ' THEN FA_ITEM.ITEM_TYPE '; 
  SQL_STR := SQL_STR || '           WHEN '  || '''' ||  IN_COLUMN_TYPE  || '''' || ' = ' || '''FA_KEYWORD1''' || ' THEN ITEM.KEYWORD1 '; 
  SQL_STR := SQL_STR || '           WHEN '  || '''' ||  IN_COLUMN_TYPE  || '''' || ' = ' || '''FA_KEYWORD2''' || ' THEN ITEM.KEYWORD2 '; 
  SQL_STR := SQL_STR || '           WHEN '  || '''' ||  IN_COLUMN_TYPE  || '''' || ' = ' || '''FA_KEYWORD3''' || ' THEN ITEM.KEYWORD3 ';   
  SQL_STR := SQL_STR || '           ELSE NULL END IN (' || GET_MULTI_VAL_STRING(IN_COLUMN_TYPE_VAL) || ')';            

  EXECUTE IMMEDIATE SQL_STR
   INTO   RET_AMOUNT;    
      
  RETURN RET_AMOUNT;
END GET_AWARD_AMT_OLD;
/
