BEGIN {FS=" "}
/slave[0-9]+/ {print $1}
END{}