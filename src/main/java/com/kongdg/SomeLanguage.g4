grammar SomeLanguage; // 声明语法头
/*
*========================
*   一些 options 配置
*=======================

*/
options {
    language = Java; //设定生成代码的语言
}
/**
 =========================
   rule
 =========================
*/
classDeclaration : 'class' className '{' (method)* '}';
className : (ID|' ')+;
method : methodName '(' (parameter)* ')'(' ')*'{' (instruction)+'}' ;
parameter:instruction|' '|'['|']';
methodName : (ID|' ')+ ;
instruction : ID|'.'|'('|')'|';';
str:STR;
ID : [a-zA-Z0-9|'_']+ ;
STR:'\'' ('\'\'' | ~('\''))* '\'';
WS: [ \t\n\r]+ -> skip ;