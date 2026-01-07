
# save as mini_calc.py and run: python mini_calc.py
import tkinter as tk
from tkinter import ttk


def compute():
    status_var.set("Calculating...")
    status_label.configure(style="Status.TLabel")
    result_text.configure(state="normal")
    result_text.delete("1.0", "end")
    result_text.configure(state="disabled")

    # Start the spinner/progress bar
    progress_bar.start(10)
    root.update_idletasks()

    try:
        a = float(entry_a.get())
        b = float(entry_b.get())
        factor = float(entry_factor.get())

        sum_ab = a + b
        product = a * b
        scaled = sum_ab * factor

        progress_bar.stop()

        # Show results
        result_text.configure(state="normal")
        result_text.insert("end", "📊 Results\n", ("title",))
        result_text.insert("end", "──────────────\n", ("sep",))

        result_text.insert("end", "➕ Sum (A + B): ", ("label",))
        result_text.insert("end", f"{sum_ab:.2f}\n", ("value",))

        result_text.insert("end", "✖ Product (A × B): ", ("label",))
        result_text.insert("end", f"{product:.2f}\n", ("value",))

        result_text.insert("end", "📈 Scaled ((A + B) × Factor): ", ("label",))
        result_text.insert("end", f"{scaled:.2f}\n", ("value",))

        result_text.configure(state="disabled")

        status_var.set("Done.")
        status_label.configure(style="StatusOK.TLabel")

    except ValueError:
        progress_bar.stop()
        status_var.set("Error: please enter valid numbers.")
        status_label.configure(style="StatusError.TLabel")


def reset_fields():
    entry_a.delete(0, "end"); entry_a.insert(0, "10")
    entry_b.delete(0, "end"); entry_b.insert(0, "5")
    entry_factor.delete(0, "end"); entry_factor.insert(0, "1.5")
    status_var.set("")
    status_label.configure(style="Status.TLabel")
    result_text.configure(state="normal")
    result_text.delete("1.0", "end")
    result_text.configure(state="disabled")


# --- Root & Style ---
root = tk.Tk()
root.title("Mini Calculator")
root.geometry("520x420")
root.minsize(480, 380)

style = ttk.Style()
try:
    style.theme_use("clam")
except tk.TclError:
    pass

# Colors & fonts
BG = "#f7f7f8"
CARD = "#ffffff"
OK = "#2e7d32"
ERR = "#c62828"
LABEL_FONT = ("Segoe UI", 10)
TITLE_FONT = ("Segoe UI", 11, "bold")

root.configure(bg=BG)

# Top header
header = ttk.Label(root, text="Mini Program: Variable-Based Calculator", font=TITLE_FONT)
header.pack(pady=(12, 6))

# Input frame
frm = ttk.Frame(root, padding=12)
frm.pack(fill="x", padx=12, pady=6)

ttk.Label(frm, text="Homes:", font=LABEL_FONT).grid(row=0, column=0, sticky="w")
entry_a = ttk.Entry(frm, width=12)
entry_a.grid(row=0, column=1, sticky="w", padx=(6, 18))
entry_a.insert(0, "10")

ttk.Label(frm, text="Workers:", font=LABEL_FONT).grid(row=0, column=2, sticky="w")
entry_b = ttk.Entry(frm, width=12)
entry_b.grid(row=0, column=3, sticky="w", padx=(6, 0))
entry_b.insert(0, "5")

ttk.Label(frm, text="Number of Outlets:", font=LABEL_FONT).grid(row=1, column=0, sticky="w", pady=(8, 0))
entry_factor = ttk.Entry(frm, width=12)
entry_factor.grid(row=1, column=1, sticky="w", padx=(6, 18), pady=(8, 0))
entry_factor.insert(0, "1.5")

# Progress / status
progress_bar = ttk.Progressbar(root, mode="indeterminate", length=360)
progress_bar.pack(pady=(8, 6))

status_var = tk.StringVar(value="")
status_label = ttk.Label(root, textvariable=status_var)
status_label.pack()

# Results card
card = ttk.Frame(root, padding=10, relief="flat")
card.pack(fill="both", expand=True, padx=12, pady=(6, 12))

result_text = tk.Text(card, height=8, wrap="word", state="disabled", bg=CARD, bd=0)
result_text.pack(fill="both", expand=True)

result_text.tag_configure("title", font=TITLE_FONT)
result_text.tag_configure("label", font=("Segoe UI", 10, "bold"))
result_text.tag_configure("value", font=("Segoe UI", 10), foreground="#111")
result_text.tag_configure("sep", foreground="#777")

# Buttons
btnfrm = ttk.Frame(root)
btnfrm.pack(pady=(6, 12))

compute_btn = ttk.Button(btnfrm, text="Compute", command=compute)
compute_btn.grid(row=0, column=0, padx=6)
reset_btn = ttk.Button(btnfrm, text="Reset", command=reset_fields)
reset_btn.grid(row=0, column=1, padx=6)

root.mainloop()
