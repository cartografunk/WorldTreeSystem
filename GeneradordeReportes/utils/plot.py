import os
from matplotlib import pyplot as plt, rcParams
from GeneradordeReportes.utils.colors import COLOR_PALETTE
from GeneradordeReportes.utils.config import EXPORT_DPI, EXPORT_WIDTH_INCHES, EXPORT_HEIGHT_INCHES

# Constantes de tamaño en cm convertido a pulgadas
FIGSIZE = (EXPORT_WIDTH_INCHES, EXPORT_HEIGHT_INCHES)


def _print_size_cm(fig):
    """Imprime dimensiones de la figura en centímetros."""
    width_in, height_in = fig.get_size_inches()
    width_cm = width_in * 2.54
    height_cm = height_in * 2.54
    print(f"📐 Tamaño final del gráfico: {width_cm:.1f}×{height_cm:.1f} cm")


def save_pie_chart(values, labels, title, output_path, colors=None, figsize=FIGSIZE, fontsize=12, show_preview=False):
    """Genera y guarda un gráfico de pastel con altura fija."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    if os.path.exists(output_path):
        print(f"⚠️ Ya existe: {output_path}")
        return

    def autopct_format(pct):
        return '' if pct in (0, 100) else (f"{pct:.0f}%" if pct.is_integer() else f"{pct:.1f}%")

    rcParams.update({'figure.autolayout': True})
    fig, ax = plt.subplots(figsize=figsize)
    ax.pie(
        values,
        labels=labels,
        colors=colors or [COLOR_PALETTE['secondary_green'], COLOR_PALETTE['primary_blue']],
        startangle=90,
        autopct=autopct_format,
        textprops={'fontsize': fontsize, 'color': COLOR_PALETTE['text_dark_gray']}
    )
    ax.set_title(title, fontsize=fontsize + 2, color=COLOR_PALETTE['primary_blue'])
    fig.patch.set_alpha(0.0)
    _print_size_cm(fig)

    fig.set_size_inches(*FIGSIZE)
    fig.savefig(
        output_path,
        dpi=EXPORT_DPI,
        facecolor='white',
        bbox_inches=None
    )
    if show_preview:
        plt.show()
    plt.close(fig)


def save_bar_chart(x_labels, series, title, output_path, ylabel="", xlabel="", colors=None, figsize=FIGSIZE, show_preview=False):
    """Genera y guarda un gráfico de barras con altura fija."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    if os.path.exists(output_path):
        print(f"⚠️ Ya existe: {output_path}")
        return

    bar_width = 0.35
    x = list(range(len(x_labels)))

    rcParams.update({'figure.autolayout': True})
    fig, ax = plt.subplots(figsize=figsize)

    for idx, (label, data) in enumerate(series.items()):
        offset = (idx - len(series) / 2) * bar_width + bar_width / 2
        bars = ax.bar(
            [i + offset for i in x],
            data,
            width=bar_width,
            label=label,
            color=(colors[idx] if colors else [COLOR_PALETTE['primary_blue'], COLOR_PALETTE['accent_yellow']][idx % 2])
        )
        for bar in bars:
            height = bar.get_height()
            ax.annotate(
                f"{height:.1f}",
                xy=(bar.get_x() + bar.get_width() / 2, height),
                xytext=(0, 3),
                textcoords="offset points",
                ha='center',
                va='bottom',
                fontsize=9,
                color=COLOR_PALETTE['text_dark_gray']
            )

    ax.set_xticks(x)
    ax.set_xticklabels(x_labels)
    ax.set_ylabel(ylabel, fontsize=12)
    ax.set_xlabel(xlabel, fontsize=12)
    ax.set_title(title, fontsize=14, color=COLOR_PALETTE['primary_blue'])
    ax.legend()
    fig.patch.set_alpha(0.0)
    plt.tight_layout()
    _print_size_cm(fig)

    fig.set_size_inches(*FIGSIZE)
    fig.savefig(
        output_path,
        dpi=EXPORT_DPI,
        facecolor='white',
        bbox_inches=None
    )
    if show_preview:
        plt.show()
    plt.close(fig)


def save_growth_candle_chart(expected, actual_values, output_path, title="Comparativa de Crecimiento", show_preview=False):
    """Genera y guarda un gráfico tipo candlestick comparativo de crecimiento."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    if os.path.exists(output_path):
        print(f"⚠️ Ya existe: {output_path}")
        return

    rcParams.update({'figure.autolayout': True})
    fig, ax = plt.subplots(figsize=FIGSIZE)

    xpos = [1, 2]
    ax.plot([xpos[0], xpos[0]], [expected['Min'], expected['Max']], color=COLOR_PALETTE['primary_blue'], linewidth=2)
    ax.scatter(xpos[0], expected['Ideal'], color=COLOR_PALETTE['primary_blue'], s=60, label="Esperado")
    ax.scatter([xpos[0], xpos[0]], [expected['Min'], expected['Max']], color=COLOR_PALETTE['primary_blue'], s=30)

    real_data = actual_values['Distribucion']
    ax.boxplot(
        real_data,
        positions=[xpos[1]],
        widths=0.3,
        patch_artist=True,
        boxprops=dict(facecolor=COLOR_PALETTE['secondary_green'], color=COLOR_PALETTE['secondary_green']),
        medianprops=dict(color='white'),
        whiskerprops=dict(color=COLOR_PALETTE['secondary_green']),
        capprops=dict(color=COLOR_PALETTE['secondary_green']),
        flierprops=dict(markerfacecolor=COLOR_PALETTE['secondary_green'], marker='o', markersize=5, linestyle='none')
    )

    ax.set_xticks(xpos)
    ax.set_xticklabels(["Esperado", "Real"])
    ax.set_ylabel("Diámetro (in)", fontsize=12)
    ax.set_title(title, fontsize=14, color=COLOR_PALETTE['primary_blue'])
    fig.patch.set_alpha(0.0)
    ax.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    _print_size_cm(fig)

    fig.set_size_inches(*FIGSIZE)
    fig.savefig(
        output_path,
        dpi=EXPORT_DPI,
        facecolor='white',
        bbox_inches=None
    )
    if show_preview:
        plt.show()
    plt.close(fig)
