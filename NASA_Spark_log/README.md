
<h1>QUESTIONÁRIO</h1> 
<h3>Qual o objetivo do comando cache em Spark?</h3>

<p>
Muitas situações, principalmente quando se trabalha com uma grande quantidade de dados, é interessante salvar resultados parciais e intermediários em memória para que possam ser reutilizados em etapas posteriores. O comando cache em spark permite o armazenamento em um cache em memória. Dessa forma, um pequeno conjunto de dados pode ser acessado repetidamente de forma eficiente (como por exemplo para algoritmos interativos como Random Forest).
</p>
<h3>
O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?
</h3>
<p>
Spark e MapReduce são soluções populares para o processamento de dados. Porém, eles trabalham de maneira diferente. O Spark pode fazer o processamento de dados em memória (RDDs), já o MapReduce precisa ler e grava em um disco. Tal fato, faz com que o Spark seja mais rápido para um mesmo código implementado em ambos os contextos. Principalmente levando em conta tarefas iterativas, em que em MapReduce há uma sobrecarga significativa.  
</p>
<h3>
Qual é a função do SparkContext? 
</h3>
<p>
Um SparkContext é um cliente do ambiente de execução do Spark e atua como o mestre de uma aplicação Spark. É o ponto de entrada para os serviços do Apache Spark (mecanismo de execução) e, portanto, o coração de um aplicativo Spark. 
O SparkContext configura serviços internos e estabelece uma conexão com um ambiente de execução do Spark. Depois que um SparkContext é criado, pode-se usá-lo para criar RDDs, acumuladores e Broadcast, acessar os serviços do Spark e executar trabalhos (até que o SparkContext seja interrompido).
</p>
<h3>
Explique com suas palavras  o que é Resilient Distributed Datasets (RDD). 
</h3>
<p>
Um RDD é, essencialmente, a representação do Spark de um conjunto de dados, distribuído em várias máquinas, com APIs para permitir que você atue sobre ele (ou seja, é uma coleção distribuída imutável de objetos). Os RDDs podem conter qualquer tipo de objetos (seja em Python, Java ou Scala), até mesmo classes. RDDs podem ser criados a partir de arquivos no HDFS ou de coleções da linguagem Scala.
</p>
<h3>
GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê? 
</h3>
<p>
Enquanto reducebykey e groupbykey produzem a mesma resposta, o reduceByKey funciona muito melhor em um grande conjunto de dados. Isso porque o Spark sabe que pode combinar a saída com uma chave comum em cada partição antes de “embaralhar” (shuffling) os dados.
Por outro lado, ao chamar groupByKey, todos os pares de valores-chave são embaralhados. Isso representa é um monte de dados desnecessários para serem transferidos pela rede. O groupByKey pode causar problemas de disco quando os dados são enviados pela rede e coletados nos reduce workes.
</p>
<h3>
Explique o que o código Scala abaixo faz.
</h3>
<p>

 ![alt text](https://github.com/fernandobcc/Challenges/blob/master/NASA_Spark_log/imagem.png)
  
</p>
<i><b>1.	val textFile = sc.textFile("hdfs://...")</b></i>
<p>
Abre arquivo de texto, cada elemento do RDD é uma linha do arquivo.
</p>
<i><b>2.	val counts = textFile.flatMap(line => line.split(" "))</b></i>
<p>
flatMap é usado para retornar cada palavra (separada por um espaço) na linha como uma array.
</p>
<i><b>3.	.map(word => (word, 1))</b></i>
<p>
Para cada palavra atribuir um valor de 1 para que elas possam ser somadas. Cria um tuple (palavra, 1)
</p>
<i><b>4.	.reduceByKey(_ + _)</b></i>
<p>
Sobre os tuples gerados na etapa anterior, através de uma agregação (somando), obtém um RDD que conta a quantidade de palavras únicas. Olhando o primeiro elemento do tuple e somando o segundo (1).  Exemplo:
(casa,1) (carro,1) (carro,1) (casa,1) (casa,1).
Após reduceByKey (casa,3) (carro,2)
</p>

<i><b>5.	counts.saveAsTextFile("hdfs://...")</b></i>
<p>
Salva o arquivo no HDFS.
Dessa forma essa função escrita em scala faz algumas transformações para construir um conjunto de dados (String, Int), em seguida, salva em um arquivo HDFS.
</p>

<h3>1. Número de hosts únicos.</h3>
<p>
Hosts unicos: 137933
</p>
<h3>2. O total de erros 404</h3>
<p>
Total de erros 404: 20899
</p>
<h3>3. Os 5 URLs que mais causaram erro 404</h3>

<table border="1">
<tr>
<th>Host</th>
<th>Total</th>
</tr>
<tr>
<td>hoohoo.ncsa.uiuc.edu</td>
<td>251</td>
</tr>
<tr>
<td>piweba3y.prodigy.com</td>
<td>157</td>
</tr>
<tr>
<td>jbiagioni.npt.nuwc.navy.mil</td>
<td>132</td>
</tr>
<tr>
<td>piweba1y.prodigy.com </td>
<td>114</td>
</tr>
<tr>
<td></td>
<td>112</td>
</tr>
</table>

<h3>4. Quantidade de erros 404 por dia</h3>


<table border="1">
<tr>
<th>Dias</th>
<th>Total (erros 404)</th>
</tr>
<tr>
<td>1</td>
<td>559</td>
</tr>
<tr>
<td>2</td>
<td>291</td>
</tr>
<tr>
<td>3</td>
<td>778</td>
</tr>
<tr>
<td>4</td>
<td>705</td>
</tr>
<tr>
<td>5</td>
<td>733</td>
</tr>
<tr>
<td>6</td>
<td>1013</td>
</tr>
<tr>
<td>7</td>
<td>1107</td>
</tr>
<tr>
<td>8</td>
<td>691</td>
</tr>
<tr>
<td>9</td>
<td>627</td>
</tr>
<tr>
<td>10</td>
<td>713</td>
</tr>
<tr>
<td>11</td>
<td>734</td>
</tr>
<tr>
<td>12</td>
<td>667</td>
</tr>
<tr>
<td>13</td>
<td>748</td>
</tr>
<tr>
<td>14</td>
<td>700</td>
</tr>
<tr>
<td>15</td>
<td>581</td>
</tr>
<tr>
<td>16</td>
<td>516</td>
</tr>
<tr>
<td>17</td>
<td>677</td>
</tr>
<tr>
<td>18</td>
<td>721</td>
</tr>
<tr>
<td>19</td>
<td>848</td>
</tr>
<tr>
<td>20</td>
<td>740</td>
</tr>
<tr>
<td>21</td>
<td>639</td>
</tr>
<tr>
<td>22</td>
<td>480</td>
</tr>
<tr>
<td>23</td>
<td>578</td>
</tr>
<tr>
<td>24</td>
<td>748</td>
</tr>
<tr>
<td>25</td>
<td>876</td>
</tr>
<tr>
<td>26</td>
<td>702</td>
</tr>
<tr>
<td>27</td>
<td>706</td>
</tr>
<tr>
<td>28</td>
<td>504</td>
</tr>
<tr>
<td>29</td>
<td>420</td>
</tr>
<tr>
<td>30</td>
<td>571</td>
</tr>
<tr>
<td>31</td>
<td>526</td>
</tr>
</table>


<h3>5. O total de bytes retornados</h3>
<table border="1">
<tr>
<th>Qtd_Bytes</th>
<th>Total_Bytes</th>
</tr>
<tr>
<td>3461613</td>
<td>65524314915</td>
</tr>
</table>


```python

```
