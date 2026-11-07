"""Las imágenes en disco: guardar y leer, la limpieza de lo vencido y el tope del volumen."""

import os

import almacen


def armar(tmp_path, vencimiento=600, max_resultados=100, maximo=10**9):
    return almacen.Almacen(str(tmp_path), vencimiento, max_resultados, maximo)


def envejecer(tmp_path, nombre, segundos):
    ruta = tmp_path / nombre
    os.utime(ruta, (ruta.stat().st_atime - segundos, ruta.stat().st_mtime - segundos))


def test_guarda_lee_y_borra(tmp_path):
    a = armar(tmp_path)
    a.guardar("abc", "entrada", b"png")
    assert a.leer("abc", "entrada") == b"png" and a.leer("abc", "resultado") is None
    a.borrar("abc", "entrada")
    a.borrar("abc", "entrada")                         # borrar lo que no está no falla
    assert a.leer("abc", "entrada") is None


def test_no_deja_temporales_al_guardar(tmp_path):
    armar(tmp_path).guardar("abc", "resultado", b"x" * 1000)
    assert sorted(os.listdir(tmp_path)) == ["abc.resultado.png"]


def test_limpia_lo_vencido_y_deja_lo_vigente(tmp_path):
    a = armar(tmp_path, vencimiento=600)
    for id_, tipo in [("viejo", "resultado"), ("nuevo", "resultado"), ("espera", "entrada"), ("huerfana", "entrada")]:
        a.guardar(id_, tipo, b"png")
    (tmp_path / "cortado.entrada.png.tmp").write_bytes(b"a medias")
    envejecer(tmp_path, "viejo.resultado.png", 601)
    envejecer(tmp_path, "espera.entrada.png", 601)     # una entrada vive 6 veces más: sigue esperando en la cola
    envejecer(tmp_path, "huerfana.entrada.png", 3601)
    envejecer(tmp_path, "cortado.entrada.png.tmp", 601)
    assert a.limpiar() == 3
    assert sorted(os.listdir(tmp_path)) == ["espera.entrada.png", "nuevo.resultado.png"]


def test_deja_como_mucho_max_resultados_y_borra_los_mas_viejos(tmp_path):
    a = armar(tmp_path, max_resultados=2)
    for i in range(4):
        a.guardar(f"r{i}", "resultado", b"png")
        envejecer(tmp_path, f"r{i}.resultado.png", 100 - i)   # r0 es el más viejo
    assert a.limpiar() == 2
    assert sorted(os.listdir(tmp_path)) == ["r2.resultado.png", "r3.resultado.png"]


def test_no_hay_lugar_si_pasaria_el_maximo_del_volumen(tmp_path):
    a = armar(tmp_path, maximo=1000)
    a.guardar("abc", "entrada", b"x" * 600)
    assert a.usado() == 600
    assert a.hay_lugar(400) and not a.hay_lugar(401)
