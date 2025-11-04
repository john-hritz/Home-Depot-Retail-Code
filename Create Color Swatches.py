from PIL import Image

# Create a new 400x400 black image
width, height = 400, 400
image = Image.new("RGB", (width, height), (220, 205, 152))  # (0, 0, 0) is black in RGB

# Save the image as a JPEG
image.save("ivory_image.jpg", "JPEG")
print("Image saved as 'ivory_image.jpg'")