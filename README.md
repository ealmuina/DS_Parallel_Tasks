# DS_Parallel_Tasks

El sistema estará equipado por computadoras dedicadas exclusivamente a este, pero deberá tener la flexibilidad para la
incorporación de computadoras nuevas y la salida de computadoras, de forma voluntaria o por la ocurrencia de fallas. La
computadora que solicita la tarea también podrá ser usada por el sistema.

El sistema podrá tener asignadas varias tareas diferentes a la vez, pero nunca deberá ocurrir que la ejecución de una
tarea en el sistema sea más lenta que su ejecución en la computadora solicitante. Se propone usar operaciones sobre
vectores y matrices para las cuales es adecuado el paralelismo de datos porque aplica la misma operación sobre cada uno
de los elementos.

La interacción del usuario con este sistema será a través de un cliente de consola, donde la interacción sea mediante
los comandos siguientes:

    exec <function> <values_file>.txt: Donde function será la operación a ejecutar en paralelo y values_file.txt será
    un archivo con los argumentos para la función, en caso de un vector serán números enteros separados por espacio, en
    caso de una matriz serán líneas de vectores que sigan la representación anterior. Este comando deberá imprimir en la
    propia consola el resultado de efectuar esta operación, acompañado del tiempo que tardó en ejecutar la función.

    stats: Este comando deberá imprimir la lista de nodos worker, identificados por un nombre único, acompañado por la
    cantidad de operaciones efectuadas y el tiempo promedio para ejecutar cada operación.


