import os
import time

# TABELA DE CORES ANSI
# (Adaptado para Python com strings de escape ANSI)
CORES = {
    "RESET": "\033[0;37m",  # Branco padrão
    "PRETO": "\033[0;30m",
    "VERMELHO": "\033[0;31m",
    "VERDE": "\033[0;32m",
    "AMARELO": "\033[0;33m",
    "AZUL": "\033[0;34m",
    "ROXO": "\033[0;35m",
    "CIANO": "\033[0;36m",
    "BRANCO": "\033[0;37m",
    "BRANCO_NEGRITO": "\033[1;37m",
    "CIANO_NEGRITO": "\033[1;36m",
    "ROXO_NEGRITO": "\033[1;35m",
}

# Variáveis globais
poswin = [[-1, -1], [-1, -1], [-1, -1]]
player = 1
char_player = ''
velha = [[' ', ' ', ' '], [' ', ' ', ' '], [' ', ' ', ' ']]

def gotoxy(x, y):
    """
    Função para posicionar o cursor na tela.
    Funciona em terminais que suportam códigos ANSI.
    """
    print(f"\033[{y+1};{x+1}H", end="")
    # Força a descarga do buffer de saída
    import sys
    sys.stdout.flush()

def espera(milliseconds):
    """
    Função de espera em milisegundos.
    """
    time.sleep(milliseconds / 1000)

def desenha_simbolo():
    """
    Desenha o símbolo do jogador atual e o número do jogador.
    """
    x, y = 42, 4
    # Limpa área do símbolo
    for yi in range(y, y + 5):
        for xi in range(x, x + 9):
            gotoxy(xi, yi)
            print(" ", end="")

    gotoxy(x, y - 2)
    print(f"Jogador  ▶", end="")

    gotoxy(x + 11, y - 2)
    if player == 1:
        print(f"{CORES['VERMELHO']}{player}{CORES['RESET']}", end="")
    else:
        print(f"{CORES['AMARELO']}{player}{CORES['RESET']}", end="")
    
    # Símbolo do jogador
    if player == 1:
        gotoxy(x, y)
        print(" █████")
        gotoxy(x, y + 1)
        print("██   ██")
        gotoxy(x, y + 2)
        print("██   ██")
        gotoxy(x, y + 3)
        print("██   ██")
        gotoxy(x, y + 4)
        print(" █████")
    else:
        gotoxy(x, y)
        print("██   ██")
        gotoxy(x, y + 1)
        print(" ██ ██")
        gotoxy(x, y + 2)
        print("   ███")
        gotoxy(x, y + 3)
        print(" ██ ██")
        gotoxy(x, y + 4)
        print("██   ██")

    print(CORES['RESET'], end="") # Reseta cor para branco

def texto(txt, x, y, clear):
    """
    Exibe um texto na tela com um atraso e opção de limpeza.
    """
    # Muda a cor para vermelho
    print(CORES['VERMELHO'], end="")
    # Posiciona o cursor
    gotoxy(x, y)
    # Exibe texto
    print(txt, end="")
    # Força a descarga do buffer de saída
    import sys
    sys.stdout.flush()
    # Aguarda 2 segundos
    espera(2000)
    # Posiciona
    gotoxy(x, y)
    # Volta para padrão branco limpando impressão
    print(CORES['RESET'], end="")
    if clear:
        # Limpa impressão conforme tamanho do texto
        print(" " * len(txt), end="")
    # Força a descarga do buffer de saída
    import sys
    sys.stdout.flush()

def exibe_credito():
    """
    Exibe os créditos do jogo.
    """
    gotoxy(60, 2)
    print(f"Projeto     : {CORES['CIANO_NEGRITO']}Jogo da velha{CORES['RESET']}", end="")
    gotoxy(60, 3)
    print(f"Autor/Aluno : {CORES['ROXO']}Adelson Corrêa Nunes - Turma NB{CORES['RESET']}", end="")
    gotoxy(60, 4)
    print(f"Professor   : {CORES['ROXO']}Otávio Augusto Lassarini Lemos{CORES['RESET']}", end="")
    gotoxy(60, 5)
    print(f"Curso       : {CORES['ROXO']}Ciência e Tecnologia{CORES['RESET']}", end="")
    gotoxy(60, 6)
    print(f"Data        : {CORES['ROXO']}18/06/2025{CORES['RESET']}", end="")

def exibe(final):
    """
    Exibe o tabuleiro do jogo.
    """
    if not final:
        gotoxy(0, 0)
        print("\n   {}Posição         Jogadas{}   ".format(CORES['VERMELHO'], CORES['CIANO']))
        print("\n  1 | 2 | 3         {} |   |  {}  ".format(CORES['BRANCO_NEGRITO'], CORES['CIANO']))
        print(" --- --- ---       --- --- --- ")
        print("  4 | 5 | 6         {} |   |  {}  ".format(CORES['BRANCO_NEGRITO'], CORES['CIANO']))
        print(" --- --- ---       --- --- ---")
        print("  7 | 8 | 9         {} |   |  {} \n{}".format(CORES['BRANCO_NEGRITO'], CORES['CIANO'], CORES['RESET']))
    else:
        gotoxy(0, 11)
        print(f"\n   {CORES['VERDE']}Vencedora{CORES['RESET']}\n")
        # Monta as jogadas
        for i in range(3):
            print(" ", end="")
            for j in range(3):
                pos_win = False
                for k in range(3):
                    # Verifica se a posição atual é uma das posições vencedoras
                    if poswin[k][0] == i and poswin[k][1] == j:
                        pos_win = True
                        break
                if pos_win:
                    # Se for uma posição vencedora, exibe em verde
                    print(f" {CORES['VERDE']}{velha[i][j]}{CORES['RESET']} ", end="")
                else:
                    # Caso contrário, exibe o caracter normal
                    print(f" {velha[i][j]} ", end="")
                if j != 2:
                    print("|", end="")
            print("") # Nova linha
            if i < 2:
                print(" --- --- ---")

    if not final:
        gotoxy(0, 10)
        print("Digite a posição ▶", end="")

    exibe_credito()

def verifica_posicao(posicao):
    """
    Verifica a disponibilidade de posicionamento e atualiza o tabuleiro.
    """
    global char_player, velha, player

    if player == 1:
        char_player = 'O'
        cor = CORES['VERMELHO']
    else:
        char_player = 'X'
        cor = CORES['AMARELO']

    result = False
    coords_map = {
        1: (0, 0, 27, 4), 2: (0, 1, 31, 4), 3: (0, 2, 35, 4),
        4: (1, 0, 27, 6), 5: (1, 1, 31, 6), 6: (1, 2, 35, 6),
        7: (2, 0, 27, 8), 8: (2, 1, 31, 8), 9: (2, 2, 35, 8),
    }

    if 1 <= posicao <= 9:
        row, col, x_coord, y_coord = coords_map[posicao]
        if velha[row][col] == ' ':
            velha[row][col] = char_player
            result = True
            gotoxy(x_coord, y_coord)
            print(f"{cor}{char_player}{CORES['RESET']}", end="")
            import sys
            sys.stdout.flush()
        else:
            texto("Posição ocupada! ", 0, 12, True)
    else:
        result = False # Posição fora do range 1-9

    return result

def ler_inteiro():
    """
    Lê um número inteiro do usuário, validando-o para ser entre 1 e 9.
    """
    desenha_simbolo()

    while True:
        # Limpa linha de digitação
        gotoxy(20, 10)
        print(" " * 80, end="")
        gotoxy(20, 10)
        
        try:
            numero = int(input().strip())
            if 1 <= numero <= 9:
                return numero
            else:
                texto("Número inválido. Digite entre 1 e 9.", 0, 12, True)
        except ValueError:
            texto("Entrada inválida. Digite apenas números inteiros.", 0, 12, True)

def venceu(posicao):
    """
    Retorna True se houver vitória e preenche poswin com as coordenadas da vitória.
    """
    global poswin, char_player

    # Zera poswin antes de verificar
    for i in range(3):
        poswin[i][0] = -1
        poswin[i][1] = -1

    # Mapeia a posição (1-9) para coordenadas (linha, coluna)
    l = (posicao - 1) // 3
    c = (posicao - 1) % 3

    # Verifica linhas
    if velha[l][0] == char_player and velha[l][1] == char_player and velha[l][2] == char_player:
        poswin[0] = [l, 0]
        poswin[1] = [l, 1]
        poswin[2] = [l, 2]
        return True
    # Verifica colunas
    if velha[0][c] == char_player and velha[1][c] == char_player and velha[2][c] == char_player:
        poswin[0] = [0, c]
        poswin[1] = [1, c]
        poswin[2] = [2, c]
        return True
    # Verifica diagonal principal
    if l == c and velha[0][0] == char_player and velha[1][1] == char_player and velha[2][2] == char_player:
        poswin[0] = [0, 0]
        poswin[1] = [1, 1]
        poswin[2] = [2, 2]
        return True
    # Verifica diagonal secundária
    if l + c == 2 and velha[0][2] == char_player and velha[1][1] == char_player and velha[2][0] == char_player:
        poswin[0] = [0, 2]
        poswin[1] = [1, 1]
        poswin[2] = [2, 0]
        return True

    return False

def sn(pergunta):
    """
    Função de pergunta Sim e Não.
    """
    while True:
        gotoxy(0, 20)
        resposta = input(f"{pergunta} (S/N): ").strip().lower()
        if resposta == 's':
            return True
        elif resposta == 'n':
            return False
        else:
            texto("Entrada inválida. Por favor, digite 'S' ou 'N'.! ", 0, 21, True)

def sair_jogo():
    """
    Verifica se o usuário deseja sair do jogo e reinicia o tabuleiro se não.
    """
    global velha, poswin, player
    sair = not sn("Continuar no jogo ")
    if not sair:
        # Limpa tela
        os.system('cls' if os.name == 'nt' else 'clear')
        # Limpa velha
        for i in range(3):
            for j in range(3):
                velha[i][j] = ' '
        # Redefine as posições vencedoras
        for i in range(3):
            poswin[i][0] = -1
            poswin[i][1] = -1
        player = 1 # Reinicia o jogador para 1
        # Mostra jogo
        exibe(False)
    return sair

def main():
    """
    Função principal do jogo.
    """
    global player, char_player, velha

    sair = False
    jogadas = 0
    
    os.system('cls' if os.name == 'nt' else 'clear')
    exibe(False)

    while not sair:
        char_player = 'O' if player == 1 else 'X'
        
        posicao = ler_inteiro()
        
        if verifica_posicao(posicao):
            jogadas += 1
            if venceu(posicao):
                exibe(True)
                print(f"\n\nJogador {player} venceu !\n\n")
                sair = sair_jogo()
                jogadas = 0 # Reinicia jogadas
            elif jogadas == 9: # Verifica empate após a última jogada, se não houver vencedor
                exibe(True)
                gotoxy(0, 10)
                print(f"\n\n{CORES['VERMELHO']}Não houve vencedor! {CORES['RESET']}\n\n")
                sair = sair_jogo()
                jogadas = 0
            else:
                player = 2 if player == 1 else 1
        # Se a posição for inválida (ocupada), o loop continua e pede nova entrada.

if __name__ == "__main__":
    main()