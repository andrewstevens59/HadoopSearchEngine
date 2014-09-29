make
../../../bin/hadoop dfs -rm  DyableParseHTML   bin/DyableParseHTML
../../../bin/hadoop dfs -put  DyableParseHTML   bin/DyableParseHTML
../../../bin/hadoop pipes -D hadoop.pipes.java.recordreader=false -D hadoop.pipes.java.recordwriter=true -program bin/DyableParseHTML -input GlobalData/Lists/StopWords -output dft18-out


