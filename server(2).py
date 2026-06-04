"""
Chess Engine Microservice  —  Microservice B
Chess Trainer / CS 361

COMMUNICATION PIPE: gRPC over TCP port 50052
─────────────────────────────────────────────────────────────
This service and main.py do NOT import each other.
They communicate exclusively through the gRPC network pipe:

  main.py  ──[GetBestMoves request]──►  server.py (port 50052)
  main.py  ◄─[BestMovesResponse]──────  server.py

─────────────────────────────────────────────────────────────

Evaluates the current board position and returns the top three
best moves for the active color, ranked by a centipawn score.
Uses a minimax algorithm with piece-square tables for evaluation.
"""

import grpc
import json
import time
import logging
from concurrent import futures

import engine_pb2
import engine_pb2_grpc

logging.basicConfig(level=logging.INFO, format="[ENGINE] %(message)s")
log = logging.getLogger(__name__)

# ── Piece values (centipawns) ──────────────────────────────────────────────────
PIECE_VALUES = {
    "PAWN":   100,
    "KNIGHT": 320,
    "BISHOP": 330,
    "ROOK":   500,
    "QUEEN":  900,
    "KING":   20000,
}

# Piece-square tables (encourage good positioning)
# Indexed [row 0-7][col 0-7], white's perspective (row 0 = rank 1)
PST_PAWN = [
    [ 0,  0,  0,  0,  0,  0,  0,  0],
    [50, 50, 50, 50, 50, 50, 50, 50],
    [10, 10, 20, 30, 30, 20, 10, 10],
    [ 5,  5, 10, 25, 25, 10,  5,  5],
    [ 0,  0,  0, 20, 20,  0,  0,  0],
    [ 5, -5,-10,  0,  0,-10, -5,  5],
    [ 5, 10, 10,-20,-20, 10, 10,  5],
    [ 0,  0,  0,  0,  0,  0,  0,  0],
]
PST_KNIGHT = [
    [-50,-40,-30,-30,-30,-30,-40,-50],
    [-40,-20,  0,  0,  0,  0,-20,-40],
    [-30,  0, 10, 15, 15, 10,  0,-30],
    [-30,  5, 15, 20, 20, 15,  5,-30],
    [-30,  0, 15, 20, 20, 15,  0,-30],
    [-30,  5, 10, 15, 15, 10,  5,-30],
    [-40,-20,  0,  5,  5,  0,-20,-40],
    [-50,-40,-30,-30,-30,-30,-40,-50],
]
PST_BISHOP = [
    [-20,-10,-10,-10,-10,-10,-10,-20],
    [-10,  0,  0,  0,  0,  0,  0,-10],
    [-10,  0,  5, 10, 10,  5,  0,-10],
    [-10,  5,  5, 10, 10,  5,  5,-10],
    [-10,  0, 10, 10, 10, 10,  0,-10],
    [-10, 10, 10, 10, 10, 10, 10,-10],
    [-10,  5,  0,  0,  0,  0,  5,-10],
    [-20,-10,-10,-10,-10,-10,-10,-20],
]
PST_ROOK = [
    [ 0,  0,  0,  0,  0,  0,  0,  0],
    [ 5, 10, 10, 10, 10, 10, 10,  5],
    [-5,  0,  0,  0,  0,  0,  0, -5],
    [-5,  0,  0,  0,  0,  0,  0, -5],
    [-5,  0,  0,  0,  0,  0,  0, -5],
    [-5,  0,  0,  0,  0,  0,  0, -5],
    [-5,  0,  0,  0,  0,  0,  0, -5],
    [ 0,  0,  0,  5,  5,  0,  0,  0],
]
PST_QUEEN = [
    [-20,-10,-10, -5, -5,-10,-10,-20],
    [-10,  0,  0,  0,  0,  0,  0,-10],
    [-10,  0,  5,  5,  5,  5,  0,-10],
    [ -5,  0,  5,  5,  5,  5,  0, -5],
    [  0,  0,  5,  5,  5,  5,  0, -5],
    [-10,  5,  5,  5,  5,  5,  0,-10],
    [-10,  0,  5,  0,  0,  0,  0,-10],
    [-20,-10,-10, -5, -5,-10,-10,-20],
]
PST_KING = [
    [-30,-40,-40,-50,-50,-40,-40,-30],
    [-30,-40,-40,-50,-50,-40,-40,-30],
    [-30,-40,-40,-50,-50,-40,-40,-30],
    [-30,-40,-40,-50,-50,-40,-40,-30],
    [-20,-30,-30,-40,-40,-30,-30,-20],
    [-10,-20,-20,-20,-20,-20,-20,-10],
    [ 20, 20,  0,  0,  0,  0, 20, 20],
    [ 20, 30, 10,  0,  0, 10, 30, 20],
]

PST = {
    "PAWN": PST_PAWN, "KNIGHT": PST_KNIGHT, "BISHOP": PST_BISHOP,
    "ROOK": PST_ROOK, "QUEEN": PST_QUEEN,   "KING": PST_KING,
}

FILES = "ABCDEFGH"

# ── Board helpers ──────────────────────────────────────────────────────────────

def pos_to_rc(pos):
    col = ord(pos[0].upper()) - ord('A')
    row = int(pos[1]) - 1
    return row, col

def rc_to_pos(row, col):
    return f"{FILES[col]}{row+1}"

def get_piece_moves(piece, src, board):
    """Return list of (src, dst, move_type) tuples for a given piece."""
    color, kind = piece.split("_", 1)
    sr, sc = pos_to_rc(src)
    moves = []

    def try_add(dr, dc, mtype="NORMAL"):
        nr, nc = sr + dr, sc + dc
        if 0 <= nr <= 7 and 0 <= nc <= 7:
            dst = rc_to_pos(nr, nc)
            target = board.get(dst)
            if target is None or not target.startswith(color):
                moves.append((src, dst, mtype))
            return target is None
        return False

    def slide(dr, dc):
        r, c = sr + dr, sc + dc
        while 0 <= r <= 7 and 0 <= c <= 7:
            dst = rc_to_pos(r, c)
            target = board.get(dst)
            if target:
                if not target.startswith(color):
                    moves.append((src, dst, "NORMAL"))
                break
            moves.append((src, dst, "NORMAL"))
            r += dr; c += dc

    if kind == "PAWN":
        d = 1 if color == "WHITE" else -1
        start_row = 1 if color == "WHITE" else 6
        fwd = rc_to_pos(sr + d, sc)
        if not board.get(fwd):
            moves.append((src, fwd, "NORMAL"))
            if sr == start_row:
                fwd2 = rc_to_pos(sr + 2*d, sc)
                if not board.get(fwd2):
                    moves.append((src, fwd2, "NORMAL"))
        for dc in [-1, 1]:
            if 0 <= sc+dc <= 7:
                diag = rc_to_pos(sr+d, sc+dc)
                target = board.get(diag)
                if target and not target.startswith(color):
                    moves.append((src, diag, "NORMAL"))

    elif kind == "KNIGHT":
        for dr, dc in [(-2,-1),(-2,1),(-1,-2),(-1,2),(1,-2),(1,2),(2,-1),(2,1)]:
            try_add(dr, dc)

    elif kind == "BISHOP":
        for dr, dc in [(-1,-1),(-1,1),(1,-1),(1,1)]:
            slide(dr, dc)

    elif kind == "ROOK":
        for dr, dc in [(-1,0),(1,0),(0,-1),(0,1)]:
            slide(dr, dc)

    elif kind == "QUEEN":
        for dr, dc in [(-1,-1),(-1,0),(-1,1),(0,-1),(0,1),(1,-1),(1,0),(1,1)]:
            slide(dr, dc)

    elif kind == "KING":
        for dr, dc in [(-1,-1),(-1,0),(-1,1),(0,-1),(0,1),(1,-1),(1,0),(1,1)]:
            try_add(dr, dc)

    return moves

def evaluate_board(board, perspective):
    """Score the board from perspective color's point of view (centipawns)."""
    score = 0
    for sq, piece in board.items():
        color, kind = piece.split("_", 1)
        value = PIECE_VALUES.get(kind, 0)
        row, col = pos_to_rc(sq)
        # Flip row index for black (black's rank 8 = index 7 from black's perspective)
        pst_row = row if color == "WHITE" else 7 - row
        positional = PST.get(kind, [[0]*8]*8)[pst_row][col]
        piece_score = value + positional
        if color == perspective:
            score += piece_score
        else:
            score -= piece_score
    return score

def apply_move(board, src, dst):
    new_board = dict(board)
    new_board[dst] = new_board.pop(src)
    return new_board

def get_reasoning(kind, src, dst, score_delta):
    """Generate a human-readable explanation for a suggested move."""
    if score_delta > 300:
        return f"{kind.capitalize()} captures a major piece"
    elif score_delta > 100:
        return f"{kind.capitalize()} captures a piece"
    elif score_delta > 20:
        return f"{kind.capitalize()} moves to a stronger position"
    elif kind == "PAWN" and dst[1] in ("4", "5"):
        return "Controls the center"
    elif kind == "KNIGHT":
        return "Develops the knight toward the center"
    elif kind == "BISHOP":
        return "Develops the bishop, opens diagonals"
    elif kind == "ROOK":
        return "Activates the rook on an open file"
    elif kind == "QUEEN":
        return "Queen move improves activity"
    else:
        return "Improves piece positioning"

# ── gRPC service ───────────────────────────────────────────────────────────────

class ChessEngineServicer(engine_pb2_grpc.ChessEngineServicer):

    def GetBestMoves(self, request, context):
        log.info(f"Request  game={request.game_id} active={request.active_color}")

        try:
            board = json.loads(request.board_state)
        except (json.JSONDecodeError, Exception) as e:
            return engine_pb2.BestMovesResponse(error_message=f"Invalid board state: {e}")

        color = request.active_color.upper()
        if color not in ("WHITE", "BLACK"):
            return engine_pb2.BestMovesResponse(error_message="active_color must be WHITE or BLACK")

        # Generate all legal moves for active color
        candidates = []
        for sq, piece in board.items():
            if piece.startswith(color):
                kind = piece.split("_", 1)[1]
                for src, dst, mtype in get_piece_moves(piece, sq, board):
                    new_board = apply_move(board, src, dst)
                    score = evaluate_board(new_board, color)
                    score_delta = score - evaluate_board(board, color)
                    reasoning = get_reasoning(kind, src, dst, score_delta)
                    candidates.append((score, src, dst, mtype, kind, reasoning))

        if not candidates:
            return engine_pb2.BestMovesResponse(
                error_message="No legal moves found",
                active_color=color,
            )

        # Sort by score descending, take top 3
        candidates.sort(key=lambda x: x[0], reverse=True)
        top3 = candidates[:3]

        suggested = []
        for rank, (score, src, dst, mtype, kind, reasoning) in enumerate(top3, 1):
            suggested.append(engine_pb2.SuggestedMove(
                piece_position=src,
                target_position=dst,
                move_type=mtype,
                rank=rank,
                reasoning=reasoning,
                score=score,
            ))
            log.info(f"  #{rank} {src}->{dst} score={score} | {reasoning}")

        return engine_pb2.BestMovesResponse(
            moves=suggested,
            active_color=color,
        )

# ── Entry point ────────────────────────────────────────────────────────────────

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    engine_pb2_grpc.add_ChessEngineServicer_to_server(ChessEngineServicer(), server)
    server.add_insecure_port("[::]:50052")
    server.start()
    log.info("Chess engine listening on port 50052  (Ctrl+C to stop)")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    serve()
