# variants
Pipeline for analyses of SARS-CoV-2 variant data and COVID-19 incidence data

## Instalação

Para executar esta pipeline, instale primeiro `conda` e na sequência `mamba`:

- Instalação do Conda: [clique aqui](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html)

- Instalação do Mamba, execute este comando, uma vez que Conda esteja instalado:
``` bash
conda install mamba -n base -c conda-forge
```
Alternativamente:
``` bash
conda install -n base conda-forge::mamba
```
*P.S.: arquivo psutil pode corromper a instalação. Se isso ocorrer, delete a pasta com psutil e reinicie instalação do mamba.*

Agora clone este repositório:
```bash
git clone https://github.com/InstitutoTodosPelaSaude/variants.git
```

Uma vez instalados `conda` e `mamba`, acesse o diretório `config`, e siga os comandos abaixo para criação dos ambientes.

**Durante toda a pipeline, dois ambientes conda são utilizados**: `var`, utilizado pra extração e transformação dos dados; e o `var-fig`, utilizado para criação dos gráficos.

#### var

Crie o ambiente `var` e instale as dependências em `variants.yaml`:

```bash
 mamba create -n var
 mamba env update -n var --file variants.yaml
 ```

#### var-fig

Crie o ambiente `var-fig` e instale as dependências em `variants-figures.yaml`:

```bash
 mamba create -n var-fig
 mamba env update -n var-fig --file variants-figures.yaml
 ```

## Execução da extração e transformação dos dados

Para executar o pipeline até o último passo da extração e transformação dos dados, execute os seguintes comandos:

1. Ative o ambiente `var`:

    ```bash
    conda activate var
    ```

2. Execute a pipeline:

    ```bash
    snakemake all --cores all
    ```
#### Linux
Este pipeline esta otimizado para ambiente MAC OS. Para executar no ambiente Linux necessário alterar as chamadas com `sed`, por exemplo no arquivo Snakefile:

```bash

# lin 222
sed -i 's/{params.unique_id}/test_result/' {output.age_matrix}

# line 398
sed -i 's/{params.test_col}/test_result/' {output}

 ```

Ref: [macOS - sed command with -i option failing on Mac, but works on Linux - Stack Overflow](https://stackoverflow.com/questions/4247068/sed-command-with-i-option-failing-on-mac-but-works-on-linux)

## Execução da criação dos gráficos
Para executar o pipeline de criação dos gráficos, execute os seguintes comandos:

1. Ative o ambiente `var-fig`:

    ```bash
    conda activate var-fig
    ```

2. Execute a pipeline:

    ```bash
    snakemake dataviz --cores all
    ```

