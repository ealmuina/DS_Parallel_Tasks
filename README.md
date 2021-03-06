# DS_Parallel_Tasks

El sistema estará equipado por computadoras dedicadas exclusivamente a este, pero deberá tener la flexibilidad para la
incorporación de computadoras nuevas y la salida de computadoras, de forma voluntaria o por la ocurrencia de fallas. La
computadora que solicita la tarea también podrá ser usada por el sistema.

El sistema podrá tener asignadas varias tareas diferentes a la vez, pero nunca deberá ocurrir que la ejecución de una
tarea en el sistema sea más lenta que su ejecución en la computadora solicitante. Se propone usar operaciones sobre
vectores y matrices para las cuales es adecuado el paralelismo de datos porque aplica la misma operación sobre cada uno
de los elementos.

Para el cliente una tarea consiste en una operación cualquiera entre dos matrices. Para la realización de la misma, se separa esta tarea en subtareas, una por cada fila, de forma tal que cada subtarea puede ser asignada a nodos distintos para realizarlas en paralelo. 
-En el caso de la operación suma y resta de matrices cada subtarea consiste en la suma o resta de los elementos de las filas i-ésimas de las matrices. 
-En el caso de la multiplicación consiste en la multiplicación de una fila de la primera matriz por la segunda matriz.

La interacción del usuario con este sistema será a través de un cliente de consola, donde la interacción sea mediante
los comandos siguientes:

    exec <function> <values_file>.txt: Donde function será la operación a ejecutar en paralelo y values_file.txt será
    un archivo con los argumentos para la función, en caso de un vector serán números enteros separados por espacio, en
    caso de una matriz serán líneas de vectores que sigan la representación anterior. Este comando deberá imprimir en la
    propia consola el resultado de efectuar esta operación, acompañado del tiempo que tardó en ejecutar la función.

    stats: Este comando deberá imprimir la lista de nodos worker, identificados por un nombre único, acompañado por la
    cantidad de operaciones efectuadas y el tiempo promedio para ejecutar cada operación.


========================================================================================================================

Componentes del sistema:

- Clientes: Son los elementos que solicitan la ejecución de tareas sobre un conjunto de datos que estos proveen.
- Nodos: Elementos del sistema, que participan en el proceso de ejecutar y responder las tareas solicitadas por los
clientes.

Un cliente tiene a su vez un nodo propio, de modo que las operaciones siempre pueden ser realizadas, incluso si el
cliente no se encuentra conectado a ninguna red compartida con otros nodos. El cliente determina los nodos existentes
en el sistema mediante un broadcast.

Los clientes mantienen conocimiento de los nodos accesibles mediante un heap de mínimo, al que añaden cada nodo; y donde
se emplea como criterio para comparar dos nodos, la cantidad de operaciones pendientes por ejecutar que tienen
multiplicada por el tiempo promedio que demora en ejecutar una operación, es decir el tiempo necesario para finalizar las operaciones pendientes y comenzar a realizar la nueva operación.

Los nodos siempre están escuchando por un puerto, a la espera de que algún cliente solicite su URI, cuando esto ocurre,
se la envían al cliente que la solicitó.

Los clientes envian subtareas a los nodos integrados al sistema para que estos la realicen. Cuando los nodos terminan estas subtareas le envían el resultado al cliente que realizó la asignación.

El cliente tiene constancia de la finalización de cada una de las subtareas. Cuando todas las subtareas de una tarea han sido completadas entonces devuelve el resultado de la misma. Cada subtarea tiene un timeout de forma tal que si ocurre este intervalo de tiempo y no se ha obtenido respuesta del nodo al que le fue asignada la subtarea, entonces se entiende que la subtarea no pudo ser realizada debido a algún problema y esta se asigna a un nuevo nodo.

¿Qué ocurre cuando un componente del sistema se desconecta?
- Si el que se desconecta es un CLIENTE, todas las tareas que este había asignado a nodos del sistema, estos las
procesarán, y al intentar reportar el resultado descubrirán que el cliente se desconectó, desechándolo y continuando sus
tareas. El cliente, por su parte, actualizará su estado para integrarse al nuevo entorno de red (de no estar conectado a
ninguna, será "localhost") y escanear en busca de nuevos nodos a los que asignar aquellas tareas para las que aún no
conoce su respuesta.
- Si el que se desconecta es un NODO, todas las tareas que le habían sido asignadas serán procesadas y al intentar
reportar su resultado, se desecharán si no puede ser localizado el cliente que las solicitó. Se actualizará su estado,
para integrarse al nuevo entorno de red y esperará los broadcasts de nuevos clientes.

