DROP FUNCTION CSMRT_OWNER.GET_MULTI_VAL_STRING
/

--
-- GET_MULTI_VAL_STRING  (Function) 
--
CREATE OR REPLACE FUNCTION CSMRT_OWNER.GET_MULTI_VAL_STRING
(
IN_COLUMN_TYPE_VAL  VARCHAR2 
)
RETURN VARCHAR2 IS 
  OUT_STR VARCHAR2(4000);

BEGIN

  -- This function returns a string of value(s) between single quotes
  -- i.e. in parm is 'ab, cd, ef' and the return string is 'ab', 'cd', 'ef' 


  OUT_STR := '''';  
  IF NVL(LENGTH(TRIM(IN_COLUMN_TYPE_VAL)), 0) > 0 THEN
     FOR I IN 1..LENGTH(IN_COLUMN_TYPE_VAL)
        LOOP
       
          IF SUBSTR(IN_COLUMN_TYPE_VAL, I, 1) = ',' THEN
             OUT_STR := OUT_STR || '''' || SUBSTR(IN_COLUMN_TYPE_VAL, I, 1) || '''';
          ELSE
             OUT_STR := OUT_STR || SUBSTR(IN_COLUMN_TYPE_VAL, I, 1) ;
          END IF; 
          
     END LOOP; 
  END IF;  
  OUT_STR := OUT_STR || ''''; 

  RETURN OUT_STR;
END GET_MULTI_VAL_STRING;
/
