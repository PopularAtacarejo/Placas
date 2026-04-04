# Automacao de Ofertas no CorelDRAW

## Fluxo principal (usuario final)

Execute `executar_ofertas.bat`.

O BAT vai:

1. Abrir a tela de login no navegador.
2. Validar usuario e senha usando `usuarios.json` no GitHub.
3. Abrir a central web no navegador.
4. Escolher a entrada na propria pagina: arquivo (`.xlsx`, `.xlsm`, `.pdf`, `.txt`) ou texto bruto colado.
5. Clicar em `Analisar` para ler os produtos e carregar a revisao.
6. Dividir em lotes de 4 produtos.
7. Na central web, ajustar produtos e selecionar quais placas (lotes) serao confeccionadas.
8. Atualizar e imprimir no CorelDRAW cada lote selecionado, em sequencia.
9. Avancar automaticamente para a proxima placa (sem confirmacao manual por placa, no BAT padrao).
10. Opcionalmente, marcar desligamento automatico do Windows ao final da impressao.
11. Por padrao, nao salva alteracoes no CDR (somente impressao).

## Interface web de configuracao

- Antes da configuracao, o sistema abre uma tela de login local no navegador.
- O login usa exclusivamente `https://github.com/PopularAtacarejo/Placas/blob/main/usuarios.json`.
- A tela de login agora tem a opcao `manter conectado por 24 horas`, salvando uma sessao local neste computador.
- Sem usuarios validos no arquivo remoto, o acesso e bloqueado.
- A revisao principal agora abre em um servidor local no navegador.
- A propria pagina aceita arquivo ou texto bruto e possui botao `Analisar`.
- O endereco e aberto automaticamente e tambem aparece no terminal como fallback.
- A tela recalcula placas em tempo real enquanto voce edita a lista.
- Se houver falha ao abrir a interface web, o sistema volta para a tela local anterior.
- O log remoto das placas e obrigatorio em `PopularAtacarejo/Placas/Ofertas de Validade.json` e exige `GITHUB_PLACAS_TOKEN`.

## Cadastro de usuarios (remoto)

Edite apenas este arquivo:

- `https://github.com/PopularAtacarejo/Placas/blob/main/usuarios.json`

Cada usuario deve ter ao menos `usuario`, `senha_hash`, `nome` e `perfil` (ou `nivel`).

`senha` em texto puro nao e aceita no login.

Na tela web, usuario com perfil `Desenvolvedor` pode criar novos usuarios e definir o nivel de acesso (`Gerador de Placas`, `Desenvolvedor` ou `Administrador`).

Para esse cadastro direto pela tela, e obrigatorio definir `GITHUB_PLACAS_TOKEN` com permissao de escrita no repositorio `PopularAtacarejo/Placas`.

Gerar hash de senha localmente:

```powershell
python gerar_hash_senha.py
```

Formato do hash: `pbkdf2_sha256$iteracoes$salt$assinatura`

Niveis aceitos para gerar placas:

- `Gerador de Placas`
- `Desenvolvedor`
- `Administrador`

Exemplo:

```json
[
  {
    "usuario": "Jeferson",
    "senha_hash": "pbkdf2_sha256$390000$SEU_SALT$SUA_ASSINATURA",
    "nome": "Jeferson",
    "perfil": "Desenvolvedor",
    "email": "djaxelf22@gmail.com",
    "ativo": true
  },
  {
    "usuario": "operador01",
    "senha_hash": "pbkdf2_sha256$390000$SEU_SALT$SUA_ASSINATURA",
    "nome": "Operador 01",
    "nivel": "Gerador de Placas",
    "email": "operador01@empresa.com",
    "ativo": true
  }
]
```

## Requisitos

1. Windows.
2. CorelDRAW instalado.
3. Python 3.10+ no `PATH`.
4. Dependencias Python:

```powershell
pip install pywin32 openpyxl pdfplumber
```

## Formato recomendado da entrada

Para maior precisao, use colunas com cabecalho:

- `Descricao`
- `Unidade`
- `Preco`

O script usa todos os produtos validos encontrados.

Tambem aceita:

- arquivo `.txt` com texto bruto copiado de mensagem;
- texto bruto colado direto na tela inicial do programa.

Exemplo:

```text
Produto Exemplo 500g
R$12,98
Validade 22/03
```

Nesse modo:

- a linha `Validade ...` e ignorada;
- cabecalhos de conversa (como exportacao do WhatsApp) sao ignorados;
- a descricao e lida a partir da linha acima do preco;
- a unidade pode ser ajustada depois na tela de revisao.

Preco aceito nos formatos `10,99` ou `10.99`.

Para PDF no formato de rebaixa (`COD. INTERNO | DESCRICAO | REBAIXA | VALIDADE`, como `Rebaixas.pdf`), a leitura ja esta preparada.

## Regras de tipografia

- Fonte da descricao e unidade: `TangoSans`.
- Tamanho da descricao: variacao automatica de `24` ate `38`.
- A descricao respeita limite de largura (`5,0 cm`) e de altura (`1,9 cm`) para preservar o respiro entre texto e preco.
- A partir de `3` palavras, o script prioriza quebra em `2 linhas`.
- Se ainda nao couber, aplica reducao controlada de fonte (ate `16 pt`) para garantir enquadramento.
- Tamanho do preco (inteiro): variacao automatica com teto de `170`.
- Preco centralizado horizontalmente com a descricao do produto.
- O preco completo tambem respeita limite vertical (`3,7 cm`) para evitar excesso de altura.
- Centavos: aplicados com tamanho menor e deslocamento vertical para manter o estilo da arte.
- Distancia inteiro->centavos: `0,1 cm` (layout separado em `PRECO_INT` + `PRECO_DEC`), com preco centralizado no card.
- Limite de altura do preco inteiro: `3,7 cm` para evitar sobreposicao/estouro no quadro.
- Unidade (`Unid.`/`Kg`): ajustada abaixo dos centavos com distancia minima de `0,28 cm`, podendo aumentar conforme a altura do preco.


## Aprendizado inteligente

- O sistema memoriza correcoes de descricao e unidade feitas na tela de revisao.
- Na proxima execucao, ele aplica essas sugestoes automaticamente quando reconhecer o produto.
- O reconhecimento agora compara o produto por texto base + assinatura de medida, tratando equivalencias como `500g` = `0,5kg` e `1L` = `1000ml`.
- Registros ruins do historico deixam de ser reaproveitados quando houver conflito evidente de descricao ou medida.
- Ele tambem memoriza historico de velocidade por template CDR e reutiliza sessao do Corel para reduzir tempo entre placas.
- Base principal de aprendizado: `PopularAtacarejo/Placas/Ofertas em Validade/aprendizado_produtos.json`.
- Fallback local: `aprendizado_produtos.json` quando o remoto nao puder ser lido ou gravado.
- Historicos: aprendizado de correcoes e `aprendizado_velocidade.json` (performance).

## IA local opcional

- O BAT principal agora chama a limpeza inteligente local automaticamente.
- Se o `Ollama` estiver instalado, o sistema tenta corrigir descricoes mais ambiguas antes da tela de revisao.
- A IA local roda apenas em itens suspeitos e respeita um limite por execucao.
- Se o `Ollama` nao estiver instalado, o fluxo segue normalmente com heuristicas e aprendizado local.

Instalacao opcional:

```powershell
ollama pull qwen3.5:0.8b
```

## Log remoto das placas

- O registro remoto das placas e obrigatorio.
- Se `GITHUB_PLACAS_TOKEN` nao estiver definido, a execucao e bloqueada.
- Destino padrao: `PopularAtacarejo/Placas`, arquivo `Ofertas de Validade.json`, branch `main`.
- Cada registro inclui apenas: usuario que produziu, data, hora e informacoes da placa (itens/produtos).
- Se falhar o envio de uma placa para o GitHub, a execucao e interrompida para nao perder rastreabilidade.

Exemplo no Windows:

```powershell
$env:GITHUB_PLACAS_TOKEN="SEU_TOKEN"
python .\atualizar_por_planilha.py --arquivo-cdr .\Validade.cdr
```

## Corretor de acentos

- O sistema aplica correcao automatica de acentos nas descricoes (ex.: `cafe` -> `café`, `acucar` -> `açúcar`, `feijao` -> `feijão`).
- O dicionario fica em `corretor_acentos.json` e pode ser editado para incluir novas palavras.

## Arquivos

- `executar_ofertas.bat`: iniciador para o usuario final.
- `atualizar_por_planilha.py`: le Excel/PDF, monta lotes de 4 e executa impressao.
- `atualizar_ofertas_corel.py`: automacao CorelDRAW (COM).
- `dados_ofertas.json`: exemplo alternativo de entrada manual JSON.

## Modo tecnico (opcional)

Sem o BAT, voce pode executar direto:

```powershell
python .\atualizar_por_planilha.py --entrada .\entrada.xlsx --arquivo-cdr .\Validade.cdr
```

Opcoes uteis:

- `--nao-imprimir`: atualiza lotes sem mandar para impressora.
- `--salvar-cdr`: salva alteracoes no CDR (padrao e sem salvar).
- `--sem-confirmacao-impressao`: nao pede confirmacao apos cada placa impressa.
- `--copias 2`: imprime 2 copias por lote.
- `--impressora "Nome da impressora"`: seleciona impressora especifica.
- `--pausa-segundos 1.5`: intervalo entre lotes.
- `--desligar-ao-final`: agenda o desligamento do Windows ao concluir a impressao de todas as placas.
- `--sem-tela-unidade`: pula a tela de revisao (descricao + unidade; e selecao de placas integrada).
- `--sem-tela-placas`: desativa a selecao manual de placas (produz todas).
- `--arquivo-aprendizado`: define o arquivo de memoria de aprendizado.
- `--arquivo-velocidade`: define o arquivo da memoria de velocidade.
- `--arquivo-corretor-acentos`: define o arquivo do dicionario de acentos.
- `--sem-modo-rapido-inteligente`: desativa a sessao continua inteligente no Corel.
- `--sem-aprendizado`: desativa leitura/gravacao do aprendizado (correcoes e velocidade).
- `--sem-corretor-acentos`: desativa a correcao automatica de acentos.
- `--usar-ia-local`: ativa limpeza local com Ollama antes da revisao.
- `--modelo-ia-local "qwen3.5:0.8b"`: escolhe o modelo local.
- `--timeout-ia-local 8`: tempo maximo por item enviado para a IA local.
- `--max-itens-ia-local 12`: limita quantos itens vao para a IA local em cada execucao.
- `--sem-log-github`: opcao legada, atualmente ignorada (log remoto obrigatorio).

## Mapeamento dos campos no Corel (opcional, mais confiavel)

Se quiser garantir 100% de mapeamento, nomeie os objetos de texto:

- `DESC_1`, `UNID_1`, `PRECO_1` (topo esquerdo)
- `DESC_2`, `UNID_2`, `PRECO_2` (topo direito)
- `DESC_3`, `UNID_3`, `PRECO_3` (baixo esquerdo)
- `DESC_4`, `UNID_4`, `PRECO_4` (baixo direito)
- Opcional para codigo de barras: `BARCODE_1..4` ou `CODIGO_BARRAS_1..4`

Se o preco estiver separado em 2 objetos:

- `PRECO_INT_1..4` e `PRECO_DEC_1..4`

Sem nomes, o script tenta detectar automaticamente.
Para a arte atual, os 4 slots de barcode tambem sao detectados automaticamente pelos objetos ja existentes no template.

## Codigo de barras vindo do PDF

- Quando o PDF vier com mais de uma coluna `CODIGO`, o parser passa a considerar como `codigo_barras` apenas o valor com exatamente `13` digitos.
- Codigos internos menores continuam sendo ignorados para o barcode.
- O campo segue para a revisao e para o CorelDRAW, que gera um EAN-13 vetorial no slot da placa.

## Diagnostico (tecnico)

Para listar todos os textos detectados no CDR:

```powershell
python .\atualizar_ofertas_corel.py --config .\dados_ofertas.json --arquivo-cdr .\Validade.cdr --diagnostico
```
